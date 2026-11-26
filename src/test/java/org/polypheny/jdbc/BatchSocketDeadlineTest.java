/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.polypheny.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.UnknownFieldSet;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CommitResponse;
import org.polypheny.prism.ConnectionCheckResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Timeout(10)
class BatchSocketDeadlineTest {

    @ParameterizedTest
    @EnumSource(BlockedRequest.class)
    void deadlineClosesConnectionWhenThePeerStopsReading( BlockedRequest request ) throws Exception {
        boolean generatedKeyPrepare = request == BlockedRequest.PREPARE_WITH_KEYS;
        boolean serverQueryTimeout = request == BlockedRequest.TIMED_STATEMENT || request == BlockedRequest.TIMED_BATCH;
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        CountDownLatch releasePeer = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.setReceiveBufferSize( 4096 );
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, generatedKeyPrepare, serverQueryTimeout );
                    // Leave the TCP receive buffer full until the client hits its deadline.
                    releasePeer.await();
                }
                return null;
            } );

            CountDownLatch writingRequest = new CountDownLatch( 1 );
            AtomicInteger transportCloses = new AtomicInteger();
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                @Override
                protected void writeEntireBuffer( ByteBuffer buffer ) throws IOException {
                    if ( buffer.remaining() > 1024 * 1024 ) {
                        writingRequest.countDown();
                        con.setOption( StandardSocketOptions.SO_SNDBUF, 4096 );
                    }
                    super.writeEntireBuffer( buffer );
                }


                @Override
                public void close() {
                    transportCloses.incrementAndGet();
                    super.close();
                }
            };
            RpcService rpc = new RpcService( transport );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), new Properties() );
                PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
                if ( generatedKeyPrepare || serverQueryTimeout ) {
                    client.register( properties, 3000 );
                }
                PolyConnection connection = new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, serverQueryTimeout ? 0 : 500 );
                Statement statement = connection.createStatement();
                if ( serverQueryTimeout ) {
                    statement.setQueryTimeout( 1 );
                }
                char[] payload = new char[8 * 1024 * 1024];
                Arrays.fill( payload, 'x' );
                String sql = "INSERT INTO probe VALUES ('" + new String( payload ) + "')";
                Future<SQLException> result;
                if ( request == BlockedRequest.PREPARE_WITH_KEYS ) {
                    result = executor.submit( () -> assertThrows( SQLException.class,
                            () -> connection.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) );
                } else if ( request == BlockedRequest.PREPARE ) {
                    result = executor.submit( () -> assertThrows( SQLException.class, () -> connection.prepareStatement( sql ) ) );
                } else if ( request == BlockedRequest.TIMED_STATEMENT ) {
                    result = executor.submit( () -> assertThrows( SQLException.class, () -> statement.executeUpdate( sql ) ) );
                } else {
                    statement.addBatch( sql );
                    result = executor.submit( () -> assertThrows( SQLException.class, statement::executeBatch ) );
                }
                assertTrue( writingRequest.await( 3, TimeUnit.SECONDS ) );
                SQLException failure = result.get( 3, TimeUnit.SECONDS );
                if ( serverQueryTimeout ) {
                    assertInstanceOf( SQLTimeoutException.class, failure );
                    assertInstanceOf( RequestDeadline.QuerySendTimeout.class, failure.getCause() );
                } else {
                    assertEquals( "08003", failure.getSQLState() );
                    assertInstanceOf( TimeoutException.class, failure.getCause() );
                }
                assertTrue( connection.isClosed() );
                assertTrue( statement.isClosed() );
                assertEquals( 1, transportCloses.get() );
            } finally {
                rpc.close();
                releasePeer.countDown();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            releasePeer.countDown();
            executor.shutdownNow();
        }
    }


    private enum BlockedRequest {
        BATCH,
        PREPARE,
        PREPARE_WITH_KEYS,
        TIMED_STATEMENT,
        TIMED_BATCH
    }


    @Test
    void completedTimedFrameLeavesItsOutcomeToTheServer() throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false, true );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request insert = readRequest( input, TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST );
                    long timeout = insert.getUnknownFields().getField( 50001 ).getVarintList().get( 0 );
                    assertTrue( timeout > 0 && timeout <= 1000 );
                    // The server can acknowledge a successful commit after the query deadline.
                    TimeUnit.MILLISECONDS.sleep( 1100 );
                    writeResponse( output, insert, responseForConcurrentOperation( insert ) );
                    assertEquals( -1, input.read() );
                }
                return null;
            } );
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                @Override
                public boolean trySendMessage( byte[] request, long deadline, BooleanSupplier beginWrite, Runnable finishWrite ) throws IOException {
                    boolean sent = super.trySendMessage( request, deadline, beginWrite, finishWrite );
                    if ( sent ) {
                        try {
                            TimeUnit.MILLISECONDS.sleep( 1250 );
                        } catch ( InterruptedException e ) {
                            Thread.currentThread().interrupt();
                            throw new IOException( e );
                        }
                    }
                    return sent;
                }
            };
            RpcService rpc = new RpcService( transport );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), new Properties() );
                PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
                client.register( properties, 3000 );
                Connection connection = new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 3000 );
                Statement statement = connection.createStatement();
                statement.setQueryTimeout( 1 );
                assertEquals( 1, statement.executeUpdate( "INSERT INTO probe VALUES (1)" ) );
                assertFalse( connection.isClosed() );
            } finally {
                rpc.close();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            executor.shutdownNow();
        }
    }


    @ParameterizedTest
    @ValueSource(ints = { 0, 2000 })
    void interruptingAnUnsentMetadataRequestDoesNotAbortPriorWork( int networkTimeout ) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        CountDownLatch reserved = new CountDownLatch( 1 );
        CountDownLatch release = new CountDownLatch( 1 );
        CountDownLatch attemptingSend = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request insert = readRequest( input, TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST );
                    writeResponse( output, insert, responseForConcurrentOperation( insert ) );
                    Request commit = readRequest( input, TypeCase.COMMIT_REQUEST );
                    writeResponse( output, commit, Response.newBuilder().setCommitResponse( CommitResponse.getDefaultInstance() ) );
                    assertEquals( -1, input.read() );
                }
                return null;
            } );
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                @Override
                public void sendMessage( byte[] request, Runnable beginWrite ) throws IOException {
                    observeMetadata( request );
                    super.sendMessage( request, beginWrite );
                }

                @Override
                public boolean trySendMessage( byte[] request, long deadline, BooleanSupplier beginWrite ) throws IOException {
                    observeMetadata( request );
                    return super.trySendMessage( request, deadline, beginWrite );
                }

                private void observeMetadata( byte[] request ) throws IOException {
                    if ( Request.parseFrom( request ).hasDefaultNamespaceRequest() ) {
                        attemptingSend.countDown();
                    }
                }
            };
            RpcService rpc = new RpcService( transport );
            Thread caller = null;
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                Properties parameters = new Properties();
                parameters.setProperty( PropertyUtils.getAUTOCOMMIT_KEY(), "false" );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), parameters );
                Connection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, networkTimeout );
                Statement statement = connection.createStatement();
                assertEquals( 1, statement.executeUpdate( "INSERT INTO probe VALUES (1)" ) );
                Future<Boolean> reservation = executor.submit( () -> rpc.withWriteReservation( 3000, () -> {
                    reserved.countDown();
                    assertTrue( release.await( 5, TimeUnit.SECONDS ) );
                } ) );
                assertTrue( reserved.await( 2, TimeUnit.SECONDS ) );

                FutureTask<SQLException> pending = new FutureTask<>( () -> {
                    SQLException failure = assertThrows( SQLException.class, () -> connection.getMetaData().getCatalogs() );
                    assertTrue( Thread.currentThread().isInterrupted() );
                    return failure;
                } );
                caller = new Thread( pending );
                caller.start();
                assertTrue( attemptingSend.await( 2, TimeUnit.SECONDS ) );
                caller.interrupt();
                assertInstanceOf( PrismInterfaceServiceException.InterruptedBeforeSend.class, pending.get( 2, TimeUnit.SECONDS ) );
                assertFalse( connection.isClosed() );
                assertFalse( statement.isClosed() );

                release.countDown();
                assertTrue( reservation.get( 2, TimeUnit.SECONDS ) );
                connection.commit();
            } finally {
                release.countDown();
                if ( caller != null ) {
                    caller.interrupt();
                }
                rpc.close();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            executor.shutdownNow();
        }
    }


    @Test
    void validityAndPrepareUseTheirDeadlinesWhileTheWriterIsReserved() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool( 3 );
        CountDownLatch reserved = new CountDownLatch( 1 );
        CountDownLatch release = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request schema = readRequest( input, TypeCase.CONNECTION_PROPERTIES_UPDATE_REQUEST );
                    assertEquals( "accepted_schema", schema.getConnectionPropertiesUpdateRequest().getConnectionProperties().getNamespaceName() );
                    writeResponse( output, schema, connectionSettingsResponse() );
                    Request check = readRequest( input, TypeCase.CONNECTION_CHECK_REQUEST );
                    writeResponse( output, check, Response.newBuilder().setConnectionCheckResponse( ConnectionCheckResponse.getDefaultInstance() ) );
                    Request prepare = readRequest( input, TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST );
                    writeResponse( output, prepare, Response.newBuilder().setPreparedStatementSignature(
                            PreparedStatementSignature.newBuilder().setStatementId( 42 ) ) );
                    assertEquals( -1, input.read() );
                }
                return null;
            } );
            RpcService rpc = new RpcService( new PlainTransport( "127.0.0.1", server.getLocalPort() ) );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort() + "/public", new Properties() );
                Connection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                Future<Boolean> reservation = executor.submit( () -> rpc.withWriteReservation( 3000, () -> {
                    reserved.countDown();
                    assertTrue( release.await( 5, TimeUnit.SECONDS ) );
                } ) );
                assertTrue( reserved.await( 2, TimeUnit.SECONDS ) );
                connection.setNetworkTimeout( Runnable::run, 300 );
                Future<Boolean> validity = executor.submit( () -> connection.isValid( 1 ) );
                connection.setAutoCommit( true );
                SQLTimeoutException failure = assertThrows( SQLTimeoutException.class, () -> connection.prepareStatement( "SELECT 1" ) );
                assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, failure.getCause() );
                SQLTimeoutException schemaFailure = assertThrows( SQLTimeoutException.class, () -> connection.setSchema( "blocked_schema" ) );
                assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, schemaFailure.getCause() );
                assertEquals( "public", connection.getSchema() );
                assertFalse( validity.get( 2, TimeUnit.SECONDS ) );
                assertFalse( connection.isClosed() );

                release.countDown();
                assertTrue( reservation.get( 2, TimeUnit.SECONDS ) );
                connection.setNetworkTimeout( Runnable::run, 500 );
                connection.setSchema( "accepted_schema" );
                assertEquals( "accepted_schema", connection.getSchema() );
                assertTrue( connection.isValid( Integer.MAX_VALUE ) );
                assertFalse( connection.prepareStatement( "SELECT 1" ).isClosed() );
            } finally {
                release.countDown();
                rpc.close();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            executor.shutdownNow();
        }
    }


    @Test
    void commitUsesOneDeadlineForWriterReservationAndReply() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool( 3 );
        CountDownLatch reserved = new CountDownLatch( 1 );
        CountDownLatch releaseWriter = new CountDownLatch( 1 );
        CountDownLatch acquiringForCommit = new CountDownLatch( 1 );
        CountDownLatch suppressReply = new CountDownLatch( 1 );
        AtomicBoolean observeCommit = new AtomicBoolean();
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request commit = readRequest( input, TypeCase.COMMIT_REQUEST );
                    if ( !suppressReply.await( 500, TimeUnit.MILLISECONDS ) ) {
                        writeResponse( output, commit, Response.newBuilder().setCommitResponse( CommitResponse.getDefaultInstance() ) );
                    }
                }
                return null;
            } );
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                @Override
                public <E extends Exception> boolean withWriteReservation( int timeout, WriteOperation<E> operation ) throws E, InterruptedException {
                    if ( observeCommit.get() ) {
                        acquiringForCommit.countDown();
                    }
                    return super.withWriteReservation( timeout, operation );
                }
            };
            RpcService rpc = new RpcService( transport );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                Properties parameters = new Properties();
                parameters.setProperty( PropertyUtils.getAUTOCOMMIT_KEY(), "false" );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), parameters );
                Connection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 850 );
                Future<Boolean> reservation = executor.submit( () -> rpc.withWriteReservation( 3000, () -> {
                    reserved.countDown();
                    assertTrue( releaseWriter.await( 3, TimeUnit.SECONDS ) );
                } ) );
                assertTrue( reserved.await( 2, TimeUnit.SECONDS ) );
                observeCommit.set( true );
                Future<SQLException> result = executor.submit( () -> assertThrows( SQLException.class, connection::commit ) );
                assertTrue( acquiringForCommit.await( 2, TimeUnit.SECONDS ) );
                TimeUnit.MILLISECONDS.sleep( 500 );
                releaseWriter.countDown();
                assertTrue( reservation.get( 2, TimeUnit.SECONDS ) );
                SQLException failure = result.get( 2, TimeUnit.SECONDS );
                assertEquals( "08003", failure.getSQLState() );
                assertInstanceOf( TimeoutException.class, failure.getCause() );
                assertTrue( connection.isClosed() );
                suppressReply.countDown();
            } finally {
                releaseWriter.countDown();
                suppressReply.countDown();
                rpc.close();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            executor.shutdownNow();
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void timelyBatchResponseDisarmsWriteWatchdogEvenIfSenderResumesAfterDeadline( boolean serverError ) throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch releasePeer = new CountDownLatch( 1 );
        CountDownLatch batchReplyProcessed = new CountDownLatch( 1 );
        AtomicLong responseProcessedAt = new AtomicLong();
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request batch = readRequest( input, Request.TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST );
                    Response.Builder response = Response.newBuilder();
                    if ( serverError ) {
                        response.setErrorResponse( ErrorResponse.newBuilder().setMessage( "Duplicate key" )
                                .setDetails( ErrorDetails.newBuilder().setState( "23505" ).setErrorCode( 37 ) ) );
                    } else {
                        response.setStatementBatchResponse( StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 ) );
                    }
                    writeResponse( output, batch, response );
                    releasePeer.await();
                }
                return null;
            } );

            AtomicInteger transportCloses = new AtomicInteger();
            AtomicBoolean pauseAfterWrite = new AtomicBoolean();
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                private boolean deliveredBatchReply;

                @Override
                public byte[] receiveMessage() throws IOException {
                    if ( deliveredBatchReply ) {
                        // The reader processed the response before it moved on to the next receive.
                        responseProcessedAt.set( System.nanoTime() );
                        batchReplyProcessed.countDown();
                    }
                    byte[] reply = super.receiveMessage();
                    Response decoded = Response.parseFrom( reply );
                    deliveredBatchReply = decoded.hasStatementBatchResponse() || decoded.hasErrorResponse();
                    return reply;
                }


                @Override
                protected void writeEntireBuffer( ByteBuffer buffer ) throws IOException {
                    super.writeEntireBuffer( buffer );
                    if ( pauseAfterWrite.compareAndSet( true, false ) ) {
                        // Simulate the sender being descheduled after the real socket write completed.
                        try {
                            assertTrue( batchReplyProcessed.await( 300, TimeUnit.MILLISECONDS ) );
                            TimeUnit.MILLISECONDS.sleep( 650 );
                        } catch ( InterruptedException e ) {
                            Thread.currentThread().interrupt();
                            throw new IOException( e );
                        }
                    }
                }


                @Override
                public void close() {
                    transportCloses.incrementAndGet();
                    super.close();
                }
            };
            RpcService rpc = new RpcService( transport );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), new Properties() );
                PolyConnection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 500 );
                Statement statement = connection.createStatement();
                statement.addBatch( "INSERT INTO probe VALUES (1)" );

                pauseAfterWrite.set( true );
                long sentAt = System.nanoTime();
                if ( serverError ) {
                    BatchUpdateException failure = assertThrows( BatchUpdateException.class, statement::executeBatch );
                    assertEquals( "23505", failure.getSQLState() );
                    assertEquals( "Duplicate key", failure.getMessage() );
                    assertEquals( 37, failure.getErrorCode() );
                } else {
                    assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
                }
                assertTrue( responseProcessedAt.get() - sentAt < TimeUnit.MILLISECONDS.toNanos( 500 ) );
                assertFalse( connection.isClosed() );
                assertEquals( 0, transportCloses.get() );
            } finally {
                rpc.close();
                releasePeer.countDown();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            releasePeer.countDown();
            executor.shutdownNow();
        }
    }


    @Test
    void queryDeadlineWhileQueuedBehindWriterLeavesTheFirstBatchAndConnectionOpen() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        CountDownLatch firstWriteStarted = new CountDownLatch( 1 );
        CountDownLatch releasePeer = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.setReceiveBufferSize( 4096 );
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    int firstRequestLength = (int) Long.reverseBytes( input.readLong() );
                    assertTrue( firstRequestLength > 2 * 1024 * 1024 );
                    byte[] firstRequest = new byte[firstRequestLength];
                    input.readFully( firstRequest, 0, 1 );
                    firstWriteStarted.countDown();
                    // Hold the first large frame in the writer's socket until the second batch expires.
                    releasePeer.await();
                    socket.setReceiveBufferSize( 256 * 1024 );
                    input.readFully( firstRequest, 1, firstRequestLength - 1 );
                    writeResponse( output, Request.parseFrom( firstRequest ), Response.newBuilder().setStatementBatchResponse(
                            StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 ) ) );
                    Request retry = readRequest( input, Request.TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST );
                    assertEquals( "INSERT INTO probe VALUES ('retry')", retry.getExecuteUnparameterizedStatementBatchRequest().getStatements( 0 ).getStatement() );
                    writeResponse( output, retry, Response.newBuilder().setStatementBatchResponse(
                            StatementBatchResponse.newBuilder().setBatchId( 42 ).addScalars( 1 ) ) );
                    assertEquals( -1, input.read() );
                }
                return null;
            } );

            AtomicInteger transportCloses = new AtomicInteger();
            PlainTransport transport = new PlainTransport( "127.0.0.1", server.getLocalPort() ) {
                @Override
                protected void writeEntireBuffer( ByteBuffer buffer ) throws IOException {
                    if ( buffer.remaining() > 1024 * 1024 ) {
                        con.setOption( StandardSocketOptions.SO_SNDBUF, 4096 );
                    }
                    super.writeEntireBuffer( buffer );
                }


                @Override
                public void close() {
                    transportCloses.incrementAndGet();
                    super.close();
                }
            };
            RpcService rpc = new RpcService( transport );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc );
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), new Properties() );
                PolyConnection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 0 );
                Statement firstStatement = connection.createStatement();
                char[] payload = new char[2 * 1024 * 1024];
                Arrays.fill( payload, 'x' );
                firstStatement.addBatch( "INSERT INTO probe VALUES ('" + new String( payload ) + "')" );
                Future<int[]> first = executor.submit( firstStatement::executeBatch );
                assertTrue( firstWriteStarted.await( 3, TimeUnit.SECONDS ) );

                Statement secondStatement = connection.createStatement();
                secondStatement.setQueryTimeout( 1 );
                secondStatement.addBatch( "INSERT INTO probe VALUES ('second')" );
                SQLTimeoutException second = assertThrows( SQLTimeoutException.class, secondStatement::executeBatch );

                assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, second.getCause() );
                assertFalse( connection.isClosed() );
                assertFalse( firstStatement.isClosed() );
                assertFalse( secondStatement.isClosed() );
                assertFalse( first.isDone() );
                assertEquals( 0, transportCloses.get() );
                releasePeer.countDown();
                assertArrayEquals( new int[]{ 1 }, first.get( 3, TimeUnit.SECONDS ) );

                secondStatement.addBatch( "INSERT INTO probe VALUES ('retry')" );
                assertArrayEquals( new int[]{ 1 }, secondStatement.executeBatch() );
            } finally {
                rpc.close();
                releasePeer.countDown();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            releasePeer.countDown();
            executor.shutdownNow();
        }
    }


    @ParameterizedTest
    @EnumSource(value = TypeCase.class, names = {
            "EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST",
            "EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST",
            "EXECUTE_INDEXED_STATEMENT_REQUEST",
            "EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST",
            "CONNECTION_PROPERTIES_UPDATE_REQUEST"
    })
    void autoCommitModeChangePrecedesConcurrentDispatch( TypeCase requestType ) throws Exception {
        boolean prepared = requestType == TypeCase.EXECUTE_INDEXED_STATEMENT_REQUEST
                || requestType == TypeCase.EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST;
        ExecutorService executor = Executors.newFixedThreadPool( 3 );
        CountDownLatch commitCompleted = new CountDownLatch( 1 );
        CountDownLatch sendModeUpdate = new CountDownLatch( 1 );
        CountDownLatch startingStatement = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeHandshake( socket, false );
                    DataInputStream input = new DataInputStream( socket.getInputStream() );
                    DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
                    Request autoCommit = readRequest( input, TypeCase.COMMIT_REQUEST );
                    writeResponse( output, autoCommit, Response.newBuilder().setCommitResponse( CommitResponse.getDefaultInstance() ) );
                    Request manual = readRequest( input, TypeCase.CONNECTION_PROPERTIES_UPDATE_REQUEST );
                    assertFalse( manual.getConnectionPropertiesUpdateRequest().getConnectionProperties().getIsAutoCommit() );
                    writeResponse( output, manual, connectionSettingsResponse() );
                    if ( prepared ) {
                        Request prepare = readRequest( input, TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST );
                        writeResponse( output, prepare, Response.newBuilder().setPreparedStatementSignature(
                                PreparedStatementSignature.newBuilder().setStatementId( 42 ) ) );
                    }
                    Request commit = readRequest( input, TypeCase.COMMIT_REQUEST );
                    writeResponse( output, commit, Response.newBuilder().setCommitResponse( CommitResponse.getDefaultInstance() ) );
                    Request mode = readRequest( input, TypeCase.CONNECTION_PROPERTIES_UPDATE_REQUEST );
                    assertTrue( mode.getConnectionPropertiesUpdateRequest().getConnectionProperties().getIsAutoCommit() );
                    assertFalse( mode.getConnectionPropertiesUpdateRequest().getConnectionProperties().hasNamespaceName() );
                    writeResponse( output, mode, connectionSettingsResponse() );
                    Request concurrent = readRequest( input, requestType );
                    writeResponse( output, concurrent, responseForConcurrentOperation( concurrent ) );
                    assertEquals( -1, input.read() );
                }
                return null;
            } );

            RpcService rpc = new RpcService( new PlainTransport( "127.0.0.1", server.getLocalPort() ) );
            try {
                PrismInterfaceClient client = new PrismInterfaceClient( rpc ) {
                    @Override
                    public void setAutoCommit( boolean autoCommit, int timeout ) throws PrismInterfaceServiceException {
                        if ( autoCommit ) {
                            commitCompleted.countDown();
                            try {
                                assertTrue( sendModeUpdate.await( 3, TimeUnit.SECONDS ) );
                            } catch ( InterruptedException e ) {
                                Thread.currentThread().interrupt();
                                throw new PrismInterfaceServiceException( e );
                            }
                        }
                        super.setAutoCommit( autoCommit, timeout );
                    }
                };
                ConnectionString target = new ConnectionString( "jdbc:polypheny://127.0.0.1:" + server.getLocalPort(), new Properties() );
                Connection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                        new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 5000 );
                connection.setAutoCommit( false );
                String sql = "INSERT INTO probe VALUES (1)";
                Statement statement = prepared ? connection.prepareStatement( sql ) : connection.createStatement();

                Future<?> modeChange = executor.submit( () -> {
                    connection.setAutoCommit( true );
                    return null;
                } );
                assertTrue( commitCompleted.await( 3, TimeUnit.SECONDS ) );

                Future<?> concurrent = executor.submit( () -> {
                    startingStatement.countDown();
                    runDuringModeSwitch( requestType, connection, statement, sql );
                    return null;
                } );
                assertTrue( startingStatement.await( 2, TimeUnit.SECONDS ) );
                assertThrows( TimeoutException.class, () -> concurrent.get( 100, TimeUnit.MILLISECONDS ) );
                if ( requestType == TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST ) {
                    try ( Statement timed = connection.createStatement() ) {
                        timed.setQueryTimeout( 1 );
                        timed.addBatch( "INSERT INTO probe VALUES (2)" );
                        SQLTimeoutException timeout = assertThrows( SQLTimeoutException.class, timed::executeBatch );
                        assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, timeout.getCause() );
                        assertFalse( connection.isClosed() );
                    }
                }

                sendModeUpdate.countDown();
                modeChange.get( 3, TimeUnit.SECONDS );
                concurrent.get( 3, TimeUnit.SECONDS );
                assertTrue( connection.getAutoCommit() );
                assertFalse( connection.isClosed() );
            } finally {
                sendModeUpdate.countDown();
                rpc.close();
            }
            peer.get( 3, TimeUnit.SECONDS );
        } finally {
            sendModeUpdate.countDown();
            executor.shutdownNow();
        }
    }


    private static void runDuringModeSwitch( TypeCase type, Connection connection, Statement statement, String sql ) throws SQLException {
        switch ( type ) {
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                assertEquals( 1, statement.executeUpdate( sql ) );
                break;
            case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST:
                statement.addBatch( sql );
                assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
                break;
            case EXECUTE_INDEXED_STATEMENT_REQUEST:
                assertEquals( 1, ((PreparedStatement) statement).executeUpdate() );
                break;
            case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST:
                ((PreparedStatement) statement).addBatch();
                assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
                break;
            case CONNECTION_PROPERTIES_UPDATE_REQUEST:
                connection.setSchema( "public" );
                break;
            default:
                throw new AssertionError( "Unexpected request: " + type );
        }
    }


    private static Response.Builder responseForConcurrentOperation( Request request ) {
        StatementResult scalar = StatementResult.newBuilder().setScalar( 1 ).build();
        switch ( request.getTypeCase() ) {
            case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                return Response.newBuilder().setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 ).setResult( scalar ) );
            case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST:
                return Response.newBuilder().setStatementBatchResponse( StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 ) );
            case EXECUTE_INDEXED_STATEMENT_REQUEST:
                return Response.newBuilder().setStatementResult( scalar );
            case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST:
                return Response.newBuilder().setStatementBatchResponse( StatementBatchResponse.newBuilder().setBatchId( 42 ).addScalars( 1 ) );
            case CONNECTION_PROPERTIES_UPDATE_REQUEST:
                assertFalse( request.getConnectionPropertiesUpdateRequest().getConnectionProperties().hasIsAutoCommit() );
                assertEquals( "public", request.getConnectionPropertiesUpdateRequest().getConnectionProperties().getNamespaceName() );
                return connectionSettingsResponse();
            default:
                throw new AssertionError( "Unexpected request: " + request.getTypeCase() );
        }
    }


    private static Response.Builder connectionSettingsResponse() {
        return Response.newBuilder().setConnectionPropertiesUpdateResponse( ConnectionPropertiesUpdateResponse.getDefaultInstance() );
    }


    private static void acknowledgeHandshake( Socket socket, boolean generatedKeyPrepare ) throws IOException {
        acknowledgeHandshake( socket, generatedKeyPrepare, false );
    }


    private static void acknowledgeHandshake( Socket socket, boolean generatedKeyPrepare, boolean serverQueryTimeout ) throws IOException {
        socket.setSoTimeout( 3000 );
        socket.setReceiveBufferSize( 4096 );
        DataInputStream input = new DataInputStream( socket.getInputStream() );
        DataOutputStream output = new DataOutputStream( socket.getOutputStream() );
        byte[] version = "plain-v1@polypheny.com\n".getBytes( StandardCharsets.US_ASCII );
        output.writeByte( version.length );
        output.write( version );
        output.flush();

        int size = input.readUnsignedByte();
        input.readFully( new byte[size] );
        if ( generatedKeyPrepare || serverQueryTimeout ) {
            Request connect = readRequest( input, Request.TypeCase.CONNECTION_REQUEST );
            ConnectionResponse.Builder handshake = ConnectionResponse.newBuilder().setIsCompatible( true ).setSupportsGeneratedKeys( generatedKeyPrepare );
            if ( serverQueryTimeout ) {
                handshake.setMajorApiVersion( 2 ).setMinorApiVersion( 5 ).setUnknownFields( UnknownFieldSet.newBuilder()
                        .addField( 50001, UnknownFieldSet.Field.newBuilder().addVarint( 1 ).build() ).build() );
            }
            writeResponse( output, connect, Response.newBuilder().setConnectionResponse( handshake ) );
            Request clientInfo = readRequest( input, Request.TypeCase.SET_CLIENT_INFO_PROPERTIES_REQUEST );
            writeResponse( output, clientInfo, Response.newBuilder().setSetClientInfoPropertiesResponse( ClientInfoPropertiesResponse.getDefaultInstance() ) );
        }
    }


    private static Request readRequest( DataInputStream input, Request.TypeCase type ) throws IOException {
        byte[] payload = new byte[(int) Long.reverseBytes( input.readLong() )];
        input.readFully( payload );
        Request request = Request.parseFrom( payload );
        assertEquals( type, request.getTypeCase() );
        return request;
    }


    private static void writeResponse( DataOutputStream output, Request request, Response.Builder builder ) throws IOException {
        byte[] response = builder.setId( request.getId() ).setLast( true ).build().toByteArray();
        output.writeLong( Long.reverseBytes( response.length ) );
        output.write( response );
        output.flush();
    }

}

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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.prism.CloseResultResponse;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.DisconnectResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Timeout(5)
class BatchCleanupTimeoutTest {

    private static final String INSERT = "INSERT INTO items DEFAULT VALUES";
    private static final String SELECT = "SELECT 1";


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void closesConnectionOnCleanupTimeoutBeforeBatch( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.TIMEOUT, false );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void queryTimeoutAppliesToCleanupWithoutNetworkTimeout( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.TIMEOUT, true );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void preservesServerErrorFromCleanupBeforeBatch( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.SERVER_ERROR, false );
    }


    private static void checkBatchCleanup( Cleanup cleanup, Intercept intercept, boolean queryOnly ) throws Exception {
        CleanupTransport transport = new CleanupTransport( cleanup.requestType, intercept );
        RpcService rpc = new RpcService( transport );
        try {
            PrismInterfaceClient client = new PrismInterfaceClient( rpc );
            try ( Connection connection = connection( client );
                    Statement statement = cleanup == Cleanup.PREPARED_RESULT ? connection.prepareStatement( SELECT ) : connection.createStatement() ) {
                if ( queryOnly ) {
                    statement.setQueryTimeout( 1 );
                } else {
                    connection.setNetworkTimeout( Runnable::run, 150 );
                }
                ResultSet previousResult = null;
                switch ( cleanup ) {
                    case RAW_BATCH:
                        statement.addBatch( INSERT );
                        assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
                        break;
                    case RAW_RESULT:
                        previousResult = statement.executeQuery( SELECT );
                        break;
                    case PREPARED_RESULT:
                        previousResult = ((PreparedStatement) statement).executeQuery();
                        break;
                }
                if ( cleanup != Cleanup.PREPARED_RESULT ) {
                    statement.addBatch( INSERT );
                }

                int requestsBeforeCleanup = transport.requests.size();
                SQLException failure = assertThrows( SQLException.class, statement::executeBatch );
                assertEquals( 1, transport.requests.size() - requestsBeforeCleanup );
                assertEquals( cleanup.requestType, transport.requests.get( requestsBeforeCleanup ) );
                if ( intercept == Intercept.SERVER_ERROR ) {
                    assertEquals( "Cannot close previous resource", failure.getMessage() );
                    assertEquals( "26000", failure.getSQLState() );
                    assertEquals( 73, failure.getErrorCode() );
                    assertNull( failure.getCause() );
                    assertFalse( connection.isClosed() );
                    assertFalse( statement.isClosed() );
                    assertEquals( 0, transport.closes.get() );
                    if ( previousResult != null ) {
                        assertFalse( previousResult.isClosed() );
                    }
                } else {
                    if ( queryOnly ) {
                        assertInstanceOf( SQLTimeoutException.class, failure );
                    } else {
                        assertEquals( "08003", failure.getSQLState() );
                    }
                    assertInstanceOf( TimeoutException.class, failure.getCause() );
                    assertTrue( connection.isClosed() );
                    assertTrue( statement.isClosed() );
                    assertEquals( 1, transport.closes.get() );
                    if ( previousResult != null ) {
                        assertTrue( previousResult.isClosed() );
                    }
                }
            }
        } finally {
            rpc.close();
        }
    }


    @Test
    void consumesTheSameQueryBudgetForCleanupAndExecution() throws Exception {
        PrismInterfaceClient client = mock( PrismInterfaceClient.class );
        AtomicLong cleanupTimeout = new AtomicLong();
        AtomicLong executionTimeout = new AtomicLong();
        doAnswer( invocation -> {
            cleanupTimeout.set( invocation.getArgument( 1, RequestDeadline.class ).remainingMillis() );
            TimeUnit.MILLISECONDS.sleep( 150 );
            return null;
        } ).when( client ).closeStatement( eq( 41 ), any( RequestDeadline.class ) );
        when( client.executeUnparameterizedStatementBatch( any(), any( RequestDeadline.class ), any() ) ).thenAnswer( invocation -> {
            executionTimeout.set( invocation.getArgument( 1, RequestDeadline.class ).remainingMillis() );
            IntConsumer acceptBatchId = invocation.getArgument( 2 );
            acceptBatchId.accept( 41 );
            return StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 ).build();
        } );
        try ( Connection connection = connection( client ); Statement statement = connection.createStatement() ) {
            statement.setQueryTimeout( 3 );
            statement.addBatch( INSERT );
            assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
            statement.addBatch( INSERT );
            assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
            assertTrue( executionTimeout.get() > 0 );
            assertTrue( cleanupTimeout.get() - executionTimeout.get() >= 100 );
        }
    }


    @Test
    void closesTheLocalCursorWhenCloseOnCompletionCannotCloseTheStatement() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.CLOSE_STATEMENT_REQUEST, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        try ( Connection connection = connection( new PrismInterfaceClient( rpc ) );
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery( SELECT ) ) {
            connection.setNetworkTimeout( Runnable::run, 150 );
            statement.closeOnCompletion();

            assertThrows( SQLException.class, result::close );
            assertTrue( connection.isClosed() );
            assertTrue( statement.isClosed() );
            assertTrue( result.isClosed() );
            assertThrows( SQLException.class, result::next );
        } finally {
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void explicitStatementCloseReleasesServerHandleAfterCursorError( boolean prepared, boolean failStatementClose ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.CLOSE_RESULT_REQUEST, Intercept.SERVER_ERROR, failStatementClose );
        RpcService rpc = new RpcService( transport );
        try ( Connection connection = connection( new PrismInterfaceClient( rpc ) );
                Statement statement = prepared ? connection.prepareStatement( SELECT ) : connection.createStatement();
                ResultSet result = prepared ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery( SELECT ) ) {
            int requestsBeforeClose = transport.requests.size();

            SQLException failure = assertThrows( SQLException.class, statement::close );

            assertEquals( Arrays.asList( TypeCase.CLOSE_RESULT_REQUEST, TypeCase.CLOSE_STATEMENT_REQUEST ),
                    transport.requests.subList( requestsBeforeClose, transport.requests.size() ) );
            assertEquals( "Cannot close previous resource", failure.getMessage() );
            assertEquals( "26000", failure.getSQLState() );
            assertEquals( 73, failure.getErrorCode() );
            assertEquals( failStatementClose ? 1 : 0, failure.getSuppressed().length );
            if ( failStatementClose ) {
                SQLException laterFailure = assertInstanceOf( SQLException.class, failure.getSuppressed()[0] );
                assertEquals( "Cannot close statement", laterFailure.getMessage() );
                assertEquals( "XX000", laterFailure.getSQLState() );
                assertEquals( 99, laterFailure.getErrorCode() );
            }
            assertFalse( connection.isClosed() );
            assertTrue( statement.isClosed() );
            assertTrue( result.isClosed() );
        } finally {
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void explicitStatementCloseRecoversFromPreSendTimeout( boolean prepared, boolean retryHandleClose ) throws Exception {
        CleanupTransport transport = new CleanupTransport( null, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = spy( new PrismInterfaceClient( rpc ) );
        doThrow( new PrismInterfaceServiceException( new RequestDeadline.BeforeSendTimeout( 1000 ) ) )
                .doCallRealMethod().when( client ).closeResult( eq( 41 ), any( RequestDeadline.class ) );
        if ( retryHandleClose ) {
            doThrow( new PrismInterfaceServiceException( new RequestDeadline.BeforeSendTimeout( 1000 ) ) )
                    .doCallRealMethod().when( client ).closeStatement( eq( 41 ), any( RequestDeadline.class ) );
        }
        try ( Connection connection = connection( client );
                Statement statement = prepared ? connection.prepareStatement( SELECT ) : connection.createStatement();
                ResultSet result = prepared ? ((PreparedStatement) statement).executeQuery() : statement.executeQuery( SELECT ) ) {
            connection.setNetworkTimeout( Runnable::run, 1000 );
            int requestsBeforeClose = transport.requests.size();

            SQLTimeoutException failure = assertThrows( SQLTimeoutException.class, statement::close );
            assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, failure.getCause() );
            assertFalse( connection.isClosed() );
            if ( retryHandleClose ) {
                assertEquals( 1, failure.getSuppressed().length );
                assertInstanceOf( SQLTimeoutException.class, failure.getSuppressed()[0] );
                assertFalse( statement.isClosed() );
                assertFalse( result.isClosed() );
                assertEquals( requestsBeforeClose, transport.requests.size() );
                statement.close();
                assertEquals( Arrays.asList( TypeCase.CLOSE_RESULT_REQUEST, TypeCase.CLOSE_STATEMENT_REQUEST ),
                        transport.requests.subList( requestsBeforeClose, transport.requests.size() ) );
            } else {
                assertEquals( Arrays.asList( TypeCase.CLOSE_STATEMENT_REQUEST ),
                        transport.requests.subList( requestsBeforeClose, transport.requests.size() ) );
            }
            assertTrue( statement.isClosed() );
            assertTrue( result.isClosed() );
        } finally {
            rpc.close();
        }
    }


    @Test
    void writeFailureClosesTheConnectionAndItsOtherCursors() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, Intercept.WRITE_FAILURE );
        RpcService rpc = new RpcService( transport );
        try ( Connection connection = connection( new PrismInterfaceClient( rpc ) );
                Statement query = connection.createStatement();
                ResultSet result = query.executeQuery( SELECT );
                Statement batch = connection.createStatement() ) {
            batch.addBatch( INSERT );

            SQLException failure = assertThrows( SQLException.class, batch::executeBatch );
            assertEquals( "08006", failure.getSQLState() );
            assertTrue( connection.isClosed() );
            assertTrue( query.isClosed() );
            assertTrue( result.isClosed() );
            assertTrue( batch.isClosed() );
            assertEquals( 1, transport.closes.get() );
        } finally {
            rpc.close();
        }
    }


    private static Connection connection( PrismInterfaceClient client ) throws SQLException {
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        return new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
    }


    private enum Intercept {
        TIMEOUT, SERVER_ERROR, WRITE_FAILURE
    }


    private enum Cleanup {
        RAW_BATCH( TypeCase.CLOSE_STATEMENT_REQUEST ),
        RAW_RESULT( TypeCase.CLOSE_RESULT_REQUEST ),
        PREPARED_RESULT( TypeCase.CLOSE_RESULT_REQUEST );

        private final TypeCase requestType;

        Cleanup( TypeCase requestType ) {
            this.requestType = requestType;
        }
    }


    private static final class CleanupTransport implements Transport {

        private final BlockingQueue<byte[]> responses = new LinkedBlockingQueue<>();
        private final List<TypeCase> requests = new ArrayList<>();
        private final byte[] endOfStream = new byte[0];
        private final AtomicInteger closes = new AtomicInteger();
        private final TypeCase cleanupType;
        private final Intercept intercept;
        private final boolean failStatementClose;
        private boolean interceptedCleanup;


        private CleanupTransport( TypeCase cleanupType, Intercept intercept ) {
            this( cleanupType, intercept, false );
        }


        private CleanupTransport( TypeCase cleanupType, Intercept intercept, boolean failStatementClose ) {
            this.cleanupType = cleanupType;
            this.intercept = intercept;
            this.failStatementClose = failStatementClose;
        }


        @Override
        public void sendMessage( byte[] message ) throws IOException {
            Request request = Request.parseFrom( message );
            TypeCase type = request.getTypeCase();
            requests.add( type );
            Response.Builder response = Response.newBuilder().setId( request.getId() ).setLast( true );
            if ( type == cleanupType && !interceptedCleanup ) {
                interceptedCleanup = true;
                switch ( intercept ) {
                    case SERVER_ERROR:
                        response.setErrorResponse( ErrorResponse.newBuilder().setMessage( "Cannot close previous resource" )
                                .setDetails( ErrorDetails.newBuilder().setState( "26000" ).setErrorCode( 73 ) ) );
                        responses.add( response.build().toByteArray() );
                        break;
                    case WRITE_FAILURE:
                        throw new IOException( "Peer closed during write" );
                    case TIMEOUT:
                        break;
                }
                return;
            }
            switch ( type ) {
                case CONNECTION_PROPERTIES_UPDATE_REQUEST:
                    response.setConnectionPropertiesUpdateResponse( ConnectionPropertiesUpdateResponse.getDefaultInstance() );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST:
                    response.setStatementBatchResponse( StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 ) );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                    response.setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 ).setResult( queryResult() ) );
                    break;
                case PREPARE_INDEXED_STATEMENT_REQUEST:
                    response.setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 41 ) );
                    break;
                case EXECUTE_INDEXED_STATEMENT_REQUEST:
                    response.setStatementResult( queryResult() );
                    break;
                case CLOSE_STATEMENT_REQUEST:
                    if ( failStatementClose ) {
                        response.setErrorResponse( ErrorResponse.newBuilder().setMessage( "Cannot close statement" )
                                .setDetails( ErrorDetails.newBuilder().setState( "XX000" ).setErrorCode( 99 ) ) );
                    } else {
                        response.setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() );
                    }
                    break;
                case CLOSE_RESULT_REQUEST:
                    response.setCloseResultResponse( CloseResultResponse.getDefaultInstance() );
                    break;
                case DISCONNECT_REQUEST:
                    response.setDisconnectResponse( DisconnectResponse.getDefaultInstance() );
                    break;
                default:
                    throw new AssertionError( "Unexpected request: " + type );
            }
            responses.add( response.build().toByteArray() );
        }


        private static StatementResult queryResult() {
            return StatementResult.newBuilder()
                    .setFrame( Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ) )
                    .build();
        }


        @Override
        public byte[] receiveMessage() throws IOException {
            try {
                byte[] result = responses.take();
                if ( result == endOfStream ) {
                    throw new EOFException();
                }
                return result;
            } catch ( InterruptedException e ) {
                Thread.currentThread().interrupt();
                throw new IOException( e );
            }
        }


        @Override
        public void close() {
            closes.incrementAndGet();
            responses.add( endOfStream );
        }

    }

}

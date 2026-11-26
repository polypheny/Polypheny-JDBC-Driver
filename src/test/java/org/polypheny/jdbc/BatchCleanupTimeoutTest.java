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

import com.google.protobuf.UnknownFieldSet;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CloseResultResponse;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.CommitResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.DisconnectResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProtoLong;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackResponse;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.TypeMeta;

@Timeout(5)
class BatchCleanupTimeoutTest {

    private static final String INSERT = "INSERT INTO items DEFAULT VALUES";
    private static final String SELECT = "SELECT 1";
    private static final int QUERY_TIMEOUT_EXTENSION = 50001;


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void closesConnectionOnCleanupTimeoutBeforeBatch( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.TIMEOUT, false, false );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void queryTimeoutAppliesToCleanupWithoutNetworkTimeout( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.TIMEOUT, true, false );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void queryTimeoutBoundsDispatchedCleanupWithServerCancellation( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.TIMEOUT, true, true );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void preservesServerErrorFromCleanupBeforeBatch( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, Intercept.SERVER_ERROR, false, false );
    }


    private static void checkBatchCleanup( Cleanup cleanup, Intercept intercept, boolean queryOnly, boolean serverQueryTimeout ) throws Exception {
        CleanupTransport transport = new CleanupTransport( cleanup.requestType, intercept );
        transport.advertiseQueryTimeout = serverQueryTimeout;
        RpcService rpc = new RpcService( transport );
        try {
            PrismInterfaceClient client = new PrismInterfaceClient( rpc );
            try ( Connection connection = connection( client, serverQueryTimeout );
                    Statement statement = cleanup == Cleanup.PREPARED_RESULT ? connection.prepareStatement( SELECT ) : connection.createStatement() ) {
                assertEquals( serverQueryTimeout, client.supportsServerQueryTimeout() );
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


    @ParameterizedTest
    @CsvSource({ "false, false", "false, true", "true, false", "true, true" })
    void connectionTimeoutDuringResultConstructionPreventsPublication( boolean prepared, boolean execute ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch buildingResult = new CountDownLatch( 1 );
        CountDownLatch resumeResult = new CountDownLatch( 1 );
        AtomicReference<Thread> queryThread = new AtomicReference<>();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        try ( Connection connection = connection( client );
                Statement query = pauseResultConstruction( connection, prepared, queryThread, buildingResult, resumeResult );
                Statement timed = connection.createStatement() ) {
            Future<?> result = executor.submit( () -> {
                queryThread.set( Thread.currentThread() );
                if ( prepared ) {
                    PreparedStatement preparedQuery = (PreparedStatement) query;
                    return execute ? preparedQuery.execute() : preparedQuery.executeQuery();
                }
                return execute ? query.execute( SELECT ) : query.executeQuery( SELECT );
            } );

            assertResultCannotBePublishedAfterAbort( connection, query, timed, result, buildingResult, resumeResult );
        } finally {
            resumeResult.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @Test
    void connectionTimeoutDuringGeneratedKeysLookupPreventsPublication() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch findingKeys = new CountDownLatch( 1 );
        CountDownLatch resumeKeys = new CountDownLatch( 1 );
        AtomicBoolean pauseLookup = new AtomicBoolean();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc ) {
            @Override
            public boolean supportsGeneratedKeys() {
                if ( pauseLookup.getAndSet( false ) ) {
                    waitToContinue( findingKeys, resumeKeys );
                }
                return true;
            }
        };
        try ( Connection connection = connection( client );
                Statement insert = connection.createStatement();
                Statement timed = connection.createStatement() ) {
            assertEquals( 1, insert.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            pauseLookup.set( true );
            Future<ResultSet> result = executor.submit( insert::getGeneratedKeys );

            assertResultCannotBePublishedAfterAbort( connection, insert, timed, result, findingKeys, resumeKeys );
        } finally {
            resumeKeys.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    private static void assertResultCannotBePublishedAfterAbort(
            Connection connection, Statement completed, Statement timed, Future<?> result,
            CountDownLatch paused, CountDownLatch resume ) throws Exception {
        assertTrue( paused.await( 2, TimeUnit.SECONDS ) );
        timed.setQueryTimeout( 1 );
        timed.addBatch( INSERT );
        assertThrows( SQLTimeoutException.class, timed::executeBatch );
        assertTrue( connection.isClosed() );
        assertTrue( completed.isClosed() );
        assertTrue( timed.isClosed() );

        resume.countDown();
        ExecutionException failure = assertThrows( ExecutionException.class, () -> result.get( 2, TimeUnit.SECONDS ) );
        SQLException sqlFailure = assertInstanceOf( SQLException.class, failure.getCause() );
        assertEquals( PrismInterfaceErrors.OPERATION_ILLEGAL.state, sqlFailure.getSQLState() );
    }


    private static void waitToContinue( CountDownLatch paused, CountDownLatch resume ) {
        paused.countDown();
        try {
            assertTrue( resume.await( 3, TimeUnit.SECONDS ) );
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new AssertionError( e );
        }
    }


    @ParameterizedTest
    @CsvSource({
            "false, 0, false, false",
            "true, 0, false, false",
            "true, 1000, false, true",
            "false, 1000, true, false"
    })
    void commitClosesAnEarlierResultEvenWhenItsCallerPublishesLate(
            boolean prepared, int networkTimeout, boolean commitFails, boolean closeOnCompletion ) throws Exception {
        CleanupTransport transport = new CleanupTransport( commitFails ? TypeCase.COMMIT_REQUEST : TypeCase.TYPE_NOT_SET, Intercept.SERVER_ERROR );
        transport.interceptedCleanup = true;
        RpcService rpc = new RpcService( transport );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch buildingResult = new CountDownLatch( 1 );
        CountDownLatch resumeResult = new CountDownLatch( 1 );
        AtomicReference<Thread> queryThread = new AtomicReference<>();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        try ( Connection connection = connection( client );
                Statement query = pauseResultConstruction( connection, prepared, queryThread, buildingResult, resumeResult ) ) {
            connection.setAutoCommit( false );
            transport.interceptedCleanup = false;
            connection.setNetworkTimeout( Runnable::run, networkTimeout );
            if ( closeOnCompletion ) {
                query.closeOnCompletion();
            }
            Future<ResultSet> pending = executor.submit( () -> {
                queryThread.set( Thread.currentThread() );
                return prepared ? ((PreparedStatement) query).executeQuery() : query.executeQuery( SELECT );
            } );
            assertTrue( buildingResult.await( 2, TimeUnit.SECONDS ) );
            if ( commitFails ) {
                assertEquals( "26000", assertThrows( SQLException.class, connection::commit ).getSQLState() );
            } else {
                connection.commit();
            }
            assertNull( query.getResultSet() );
            resumeResult.countDown();

            try ( ResultSet result = pending.get( 2, TimeUnit.SECONDS ) ) {
                if ( commitFails ) {
                    assertFalse( result.isClosed() );
                    assertEquals( result, query.getResultSet() );
                } else {
                    assertTrue( result.isClosed() );
                    assertThrows( SQLException.class, result::next );
                    assertEquals( closeOnCompletion, query.isClosed() );
                    if ( !closeOnCompletion ) {
                        assertNull( query.getResultSet() );
                        try ( ResultSet later = prepared ? ((PreparedStatement) query).executeQuery() : query.executeQuery( SELECT ) ) {
                            assertFalse( later.isClosed() );
                        }
                    }
                }
            }
            assertFalse( connection.isClosed() );
        } finally {
            resumeResult.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @Test
    void commitWithoutReplyDoesNotAdvanceTheCommitGeneration() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.COMMIT_REQUEST, Intercept.TIMEOUT );
        transport.interceptedCleanup = true;
        RpcService rpc = new RpcService( transport );
        try ( Connection connection = connection( new PrismInterfaceClient( rpc ) ) ) {
            connection.setAutoCommit( false );
            transport.interceptedCleanup = false;
            connection.setNetworkTimeout( Runnable::run, 100 );
            SQLException failure = assertThrows( SQLException.class, connection::commit );
            assertInstanceOf( TimeoutException.class, failure.getCause() );
            assertTrue( connection.isClosed() );
            assertEquals( 1, rpc.getTransactionGeneration() );
        } finally {
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false, false", "true, false, true", "false, true, false", "true, true, false" })
    void commitDiscardsEarlierGeneratedKeysEvenWhenTheExecutionReturnsLate(
            boolean prepared, boolean batch, boolean closeOnCompletion ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        transport.returnGeneratedKeys = true;
        RpcService rpc = new RpcService( transport );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch executionReturned = new CountDownLatch( 1 );
        CountDownLatch resumeExecution = new CountDownLatch( 1 );
        AtomicReference<Thread> executionThread = new AtomicReference<>();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc ) {
            @Override
            public boolean supportsGeneratedKeys() {
                return true;
            }


            @Override
            void discardCallback( CallbackQueue<?> callback ) {
                super.discardCallback( callback );
                pauseExecution();
            }


            @Override
            StatementResult executeIndexedStatement( int statementId, List<TypedValue> values, int fetchSize, RequestDeadline deadline ) throws PrismInterfaceServiceException {
                StatementResult result = super.executeIndexedStatement( statementId, values, fetchSize, deadline );
                pauseExecution();
                return result;
            }


            @Override
            StatementBatchResponse executeIndexedStatementBatch( int statementId, List<List<TypedValue>> bindings, RequestDeadline deadline ) throws PrismInterfaceServiceException {
                StatementBatchResponse result = super.executeIndexedStatementBatch( statementId, bindings, deadline );
                pauseExecution();
                return result;
            }


            @Override
            StatementBatchResponse executeUnparameterizedStatementBatch(
                    List<ExecuteUnparameterizedStatementRequest> requests, RequestDeadline deadline, IntConsumer acceptBatchId ) throws PrismInterfaceServiceException {
                StatementBatchResponse result = super.executeUnparameterizedStatementBatch( requests, deadline, acceptBatchId );
                pauseExecution();
                return result;
            }


            private void pauseExecution() {
                if ( Thread.currentThread() == executionThread.get() ) {
                    waitToContinue( executionReturned, resumeExecution );
                }
            }
        };
        try ( Connection connection = connection( client );
                Statement write = prepared ? connection.prepareStatement( INSERT, Statement.RETURN_GENERATED_KEYS ) : connection.createStatement() ) {
            connection.setAutoCommit( false );
            if ( closeOnCompletion ) {
                write.closeOnCompletion();
            }
            Future<Integer> count = executor.submit( () -> {
                executionThread.set( Thread.currentThread() );
                if ( batch ) {
                    if ( prepared ) {
                        ((PreparedStatement) write).addBatch();
                    } else {
                        write.addBatch( INSERT );
                    }
                    return write.executeBatch()[0];
                }
                return prepared ? ((PreparedStatement) write).executeUpdate() : write.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS );
            } );
            assertTrue( executionReturned.await( 2, TimeUnit.SECONDS ) );
            connection.commit();
            resumeExecution.countDown();

            assertEquals( 1, count.get( 2, TimeUnit.SECONDS ) );
            assertEquals( closeOnCompletion, write.isClosed() );
            if ( !closeOnCompletion ) {
                try ( ResultSet keys = write.getGeneratedKeys() ) {
                    assertFalse( keys.next() );
                }
            }
            assertFalse( connection.isClosed() );
        } finally {
            resumeExecution.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "true, true" })
    void transactionEndClosesPreviouslyPublishedResults( boolean implicit, boolean fails ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties settings = new PolyphenyConnectionProperties( target, client );
        client.register( settings, 1000 );
        try ( Connection connection = new PolyConnection( settings, new PolyphenyDatabaseMetadata( client, target ) );
                Statement query = connection.createStatement(); Statement insert = connection.createStatement();
                Statement ddl = connection.createStatement() ) {
            assertTrue( transport.boundaryEventsRequested );
            connection.setAutoCommit( false );
            query.closeOnCompletion();
            ResultSet rows = query.executeQuery( SELECT );
            assertEquals( 1, insert.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            ResultSet keys = insert.getGeneratedKeys();

            if ( implicit ) {
                transport.sendImplicitBoundary = true;
                transport.implicitBoundaryFails = fails;
                if ( fails ) {
                    SQLException error = assertThrows( SQLException.class, () -> ddl.executeUpdate( "CREATE TABLE probe (x INTEGER)" ) );
                    assertEquals( "42P16", error.getSQLState() );
                } else {
                    assertEquals( 1, ddl.executeUpdate( "CREATE TABLE probe (x INTEGER PRIMARY KEY)" ) );
                }
            } else {
                connection.rollback();
            }

            assertTrue( rows.isClosed() );
            assertTrue( query.isClosed() );
            assertTrue( keys.isClosed() );
            assertThrows( SQLException.class, rows::next );
            assertThrows( SQLException.class, keys::next );
            assertFalse( insert.isClosed() );
            assertFalse( connection.isClosed() );
            try ( ResultSet empty = insert.getGeneratedKeys() ) {
                assertFalse( empty.next() );
            }
            assertEquals( 1, insert.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            try ( ResultSet later = insert.getGeneratedKeys() ) {
                assertTrue( later.next() );
            }
        } finally {
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false, false", "false, true, false", "true, false, false", "true, true, false", "false, true, true", "true, true, true" })
    void checksCompletionWhenAnotherJdbcCallerHasNotSweptTheTransaction( boolean generatedKeys, boolean closeOnCompletion, boolean lateOptIn ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties settings = new PolyphenyConnectionProperties( target, client );
        CountDownLatch boundaryObserved = new CountDownLatch( 1 );
        CountDownLatch resumeSweep = new CountDownLatch( 1 );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> ddlThread = new AtomicReference<>();
        client.register( settings, 1000 );
        try ( Connection connection = new PolyConnection( settings, new PolyphenyDatabaseMetadata( client, target ) ) {
            @Override
            public void refreshTransactionResults() {
                if ( Thread.currentThread() == ddlThread.get() ) {
                    waitToContinue( boundaryObserved, resumeSweep );
                }
                super.refreshTransactionResults();
            }
        }; Statement dependent = connection.createStatement(); Statement ddl = connection.createStatement() ) {
            connection.setAutoCommit( false );
            if ( closeOnCompletion && !lateOptIn ) {
                dependent.closeOnCompletion();
            }
            ResultSet result;
            if ( generatedKeys ) {
                assertEquals( 1, dependent.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
                result = dependent.getGeneratedKeys();
            } else {
                result = dependent.executeQuery( SELECT );
            }
            transport.sendImplicitBoundary = true;
            Future<Integer> boundary = executor.submit( () -> {
                ddlThread.set( Thread.currentThread() );
                return ddl.executeUpdate( "CREATE TABLE probe (x INTEGER PRIMARY KEY)" );
            } );
            try {
                assertTrue( boundaryObserved.await( 2, TimeUnit.SECONDS ) );
                assertTrue( result.isClosed() );
                if ( lateOptIn ) {
                    assertFalse( dependent.isClosed() );
                    dependent.closeOnCompletion();
                }
                int sent = transport.requests.size();
                if ( closeOnCompletion && !lateOptIn ) {
                    assertTrue( dependent.isClosed() );
                    assertThrows( SQLException.class, () -> dependent.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
                    assertEquals( sent, transport.requests.size() );
                } else {
                    assertFalse( dependent.isClosed() );
                    assertEquals( 1, dependent.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
                    assertTrue( transport.requests.size() > sent );
                }
            } finally {
                resumeSweep.countDown();
            }
            assertEquals( 1, boundary.get( 2, TimeUnit.SECONDS ) );
            assertEquals( closeOnCompletion && !lateOptIn, dependent.isClosed() );
            if ( lateOptIn ) {
                try ( ResultSet followingKeys = dependent.getGeneratedKeys() ) {
                    assertTrue( followingKeys.next() );
                }
                assertTrue( dependent.isClosed() );
            }
            assertFalse( connection.isClosed() );
        } finally {
            resumeSweep.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @ParameterizedTest
    @CsvSource({ "false", "true" })
    void multimodelBoundaryReleasesCompletedJdbcStatement( boolean cleanupMustRetry ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties settings = new PolyphenyConnectionProperties( target, client );
        client.register( settings, 1000 );
        try ( PolyConnection connection = new PolyConnection( settings, new PolyphenyDatabaseMetadata( client, target ) );
                Statement query = connection.createStatement(); PolyStatement ddl = connection.createPolyStatement() ) {
            connection.setAutoCommit( false );
            connection.setNetworkTimeout( Runnable::run, 1000 );
            query.closeOnCompletion();
            try ( ResultSet result = query.executeQuery( SELECT ) ) {
                transport.sendImplicitBoundary = true;
                transport.rejectStatementCloseBeforeSend = cleanupMustRetry;
                ddl.execute( "public", "sql", "CREATE TABLE probe (x INTEGER PRIMARY KEY)" );
                assertEquals( cleanupMustRetry ? 0 : 1, transport.requests.stream().filter( TypeCase.CLOSE_STATEMENT_REQUEST::equals ).count() );
                assertTrue( result.isClosed() );
                assertTrue( query.isClosed() );
                assertThrows( SQLException.class, () -> query.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
                assertFalse( connection.isClosed() );

                transport.rejectStatementCloseBeforeSend = false;
                query.close();
                assertEquals( 1, transport.requests.stream().filter( TypeCase.CLOSE_STATEMENT_REQUEST::equals ).count() );
            }
        } finally {
            rpc.close();
        }
    }


    @ParameterizedTest
    @EnumSource( GeneratedKeysAccess.class )
    void commitOnlyCompletesGeneratedKeyResultsStillOpen( GeneratedKeysAccess access ) throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties settings = new PolyphenyConnectionProperties( target, client );
        CountDownLatch boundaryObserved = new CountDownLatch( 1 );
        CountDownLatch resumeSweep = new CountDownLatch( 1 );
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> commitThread = new AtomicReference<>();
        client.register( settings, 1000 );
        try ( Connection connection = new PolyConnection( settings, new PolyphenyDatabaseMetadata( client, target ) ) {
            @Override
            public void refreshTransactionResults() {
                if ( Thread.currentThread() == commitThread.get() ) {
                    waitToContinue( boundaryObserved, resumeSweep );
                }
                super.refreshTransactionResults();
            }
        }; Statement insert = connection.createStatement() ) {
            connection.setAutoCommit( false );
            assertEquals( 1, insert.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            boolean alreadyClosed = access == GeneratedKeysAccess.ALREADY_CLOSED;
            try ( ResultSet keys = access == GeneratedKeysAccess.UNRETRIEVED ? null : insert.getGeneratedKeys() ) {
                if ( alreadyClosed ) {
                    keys.close();
                }
                insert.closeOnCompletion();
                assertFalse( insert.isClosed() );
                Future<?> committed = executor.submit( () -> {
                    commitThread.set( Thread.currentThread() );
                    connection.commit();
                    return null;
                } );
                try {
                    assertTrue( boundaryObserved.await( 2, TimeUnit.SECONDS ) );
                    if ( keys != null ) {
                        assertTrue( keys.isClosed() );
                    }
                    assertEquals( !alreadyClosed, insert.isClosed() );
                } finally {
                    resumeSweep.countDown();
                }
                committed.get( 2, TimeUnit.SECONDS );
                assertEquals( !alreadyClosed, insert.isClosed() );
                if ( alreadyClosed ) {
                    assertEquals( 1, insert.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
                    try ( ResultSet followingKeys = insert.getGeneratedKeys() ) {
                        assertTrue( followingKeys.next() );
                    }
                    assertTrue( insert.isClosed() );
                }
            }
        } finally {
            resumeSweep.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @Test
    void queryTimeoutReturnsBeforeUnrelatedPostBoundaryCleanup() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        transport.advertiseQueryTimeout = true;
        transport.holdUpdatesAndTheirFollowingClose = true;
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties settings = new PolyphenyConnectionProperties( target, client );
        CountDownLatch boundaryObserved = new CountDownLatch( 1 );
        CountDownLatch resumeSweep = new CountDownLatch( 1 );
        ExecutorService executor = Executors.newFixedThreadPool( 3 );
        AtomicReference<Thread> ddlThread = new AtomicReference<>();
        client.register( settings, 1000 );
        try ( Connection connection = new PolyConnection( settings, new PolyphenyDatabaseMetadata( client, target ) ) {
            @Override
            public void refreshTransactionResults() {
                if ( Thread.currentThread() == ddlThread.get() ) {
                    waitToContinue( boundaryObserved, resumeSweep );
                }
                super.refreshTransactionResults();
            }
        }; Statement dependent = connection.createStatement(); Statement ddl = connection.createStatement();
                Statement blocker = connection.createStatement(); Statement timed = connection.createStatement() ) {
            connection.setAutoCommit( false );
            dependent.closeOnCompletion();
            try ( ResultSet result = dependent.executeQuery( SELECT ) ) {
                assertFalse( result.isClosed() );
                transport.sendImplicitBoundary = true;
                Future<Integer> ddlResult = executor.submit( () -> {
                    ddlThread.set( Thread.currentThread() );
                    return ddl.executeUpdate( "CREATE TABLE probe (x INTEGER PRIMARY KEY)" );
                } );
                assertTrue( boundaryObserved.await( 2, TimeUnit.SECONDS ) );
                assertTrue( result.isClosed() );
                Future<Integer> preceding = executor.submit( () -> blocker.executeUpdate( "UPDATE items SET x = 1" ) );
                assertTrue( transport.blockedUpdateSent.await( 1, TimeUnit.SECONDS ) );
                timed.setQueryTimeout( 1 );
                Future<Integer> expired = executor.submit( () -> timed.executeUpdate( "UPDATE items SET x = 2" ) );
                try {
                    ExecutionException failure = assertThrows( ExecutionException.class, () -> expired.get( 2, TimeUnit.SECONDS ) );
                    assertInstanceOf( SQLTimeoutException.class, failure.getCause() );
                    assertTrue( transport.queuedCloseSent.await( 1, TimeUnit.SECONDS ) );
                    assertFalse( preceding.isDone() );
                    assertTrue( dependent.isClosed() );
                    assertFalse( connection.isClosed() );
                } finally {
                    transport.releaseBlockedUpdateAndClose();
                    resumeSweep.countDown();
                }
                assertEquals( 1, preceding.get( 1, TimeUnit.SECONDS ) );
                assertEquals( 1, ddlResult.get( 1, TimeUnit.SECONDS ) );
            }
        } finally {
            transport.releaseBlockedUpdateAndClose();
            resumeSweep.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    @Test
    void completedExecutionReturnsWhileLateResultHandleIsClosing() throws Exception {
        CleanupTransport transport = new CleanupTransport( TypeCase.TYPE_NOT_SET, Intercept.TIMEOUT );
        transport.holdUpdatesAndTheirFollowingClose = true;
        RpcService rpc = new RpcService( transport );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        CountDownLatch buildingResult = new CountDownLatch( 1 );
        CountDownLatch resumeResult = new CountDownLatch( 1 );
        AtomicReference<Thread> queryThread = new AtomicReference<>();
        try ( Connection connection = connection( client );
                Statement query = pauseResultConstruction( connection, false, queryThread, buildingResult, resumeResult );
                Statement blocker = connection.createStatement() ) {
            connection.setAutoCommit( false );
            connection.setNetworkTimeout( Runnable::run, 1000 );
            query.closeOnCompletion();
            Future<ResultSet> pending = executor.submit( () -> {
                queryThread.set( Thread.currentThread() );
                return query.executeQuery( SELECT );
            } );
            assertTrue( buildingResult.await( 2, TimeUnit.SECONDS ) );
            connection.commit();
            connection.setNetworkTimeout( Runnable::run, 0 );
            Future<Integer> preceding = executor.submit( () -> blocker.executeUpdate( "UPDATE items SET x = 1" ) );
            assertTrue( transport.blockedUpdateSent.await( 1, TimeUnit.SECONDS ) );
            resumeResult.countDown();
            try {
                assertTrue( transport.queuedCloseSent.await( 2, TimeUnit.SECONDS ) );
                assertTrue( query.isClosed() );
                try ( ResultSet result = pending.get( 1, TimeUnit.SECONDS ) ) {
                    assertTrue( result.isClosed() );
                }
                assertFalse( preceding.isDone() );
            } finally {
                transport.releaseBlockedUpdateAndClose();
            }
            assertEquals( 1, preceding.get( 1, TimeUnit.SECONDS ) );
        } finally {
            transport.releaseBlockedUpdateAndClose();
            resumeResult.countDown();
            executor.shutdownNow();
            rpc.close();
        }
    }


    private static Statement pauseResultConstruction( Connection connection, boolean prepared, AtomicReference<Thread> queryThread,
            CountDownLatch buildingResult, CountDownLatch resumeResult ) throws SQLException {
        PolyphenyStatement original = (PolyphenyStatement) (prepared ? connection.prepareStatement( SELECT ) : connection.createStatement());
        PolyphenyStatement statement = spy( original );
        PolyConnection owner = original.getPolyConnection();
        owner.endTracking( original );
        owner.startTracking( statement );
        doAnswer( invocation -> {
            if ( Thread.currentThread() == queryThread.get() ) {
                waitToContinue( buildingResult, resumeResult );
            }
            return owner;
        } ).when( statement ).getPolyConnection();
        return statement;
    }


    private static Connection connection( PrismInterfaceClient client ) throws SQLException {
        return connection( client, false );
    }


    private static Connection connection( PrismInterfaceClient client, boolean register ) throws SQLException {
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        if ( register ) {
            client.register( properties, 1000 );
        }
        return new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
    }


    private enum Intercept {
        TIMEOUT, SERVER_ERROR, WRITE_FAILURE
    }


    private enum GeneratedKeysAccess {
        UNRETRIEVED, OPEN, ALREADY_CLOSED
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
        private boolean returnGeneratedKeys;
        private boolean boundaryEventsRequested;
        private boolean sendImplicitBoundary;
        private boolean implicitBoundaryFails;
        private boolean rejectStatementCloseBeforeSend;
        private boolean advertiseQueryTimeout;


        private boolean holdUpdatesAndTheirFollowingClose;
        private final CountDownLatch blockedUpdateSent = new CountDownLatch( 1 );
        private final CountDownLatch queuedCloseSent = new CountDownLatch( 1 );
        private volatile long blockedUpdateId;
        private volatile long queuedCloseId;

        private void releaseBlockedUpdateAndClose() {
            long updateId = blockedUpdateId;
            long closeId = queuedCloseId;
            blockedUpdateId = 0;
            queuedCloseId = 0;
            if ( updateId != 0 ) {
                responses.add( Response.newBuilder().setId( updateId ).setLast( true )
                        .setStatementResponse( StatementResponse.newBuilder().setStatementId( 43 )
                                .setResult( StatementResult.newBuilder().setScalar( 1 ) ) ).build().toByteArray() );
            }
            if ( closeId != 0 ) {
                responses.add( Response.newBuilder().setId( closeId ).setLast( true )
                        .setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() ).build().toByteArray() );
            }
        }


        private CleanupTransport( TypeCase cleanupType, Intercept intercept ) {
            this( cleanupType, intercept, false );
        }


        private CleanupTransport( TypeCase cleanupType, Intercept intercept, boolean failStatementClose ) {
            this.cleanupType = cleanupType;
            this.intercept = intercept;
            this.failStatementClose = failStatementClose;
        }


        @Override
        public boolean trySendMessage( byte[] message, long deadlineNanos, BooleanSupplier beginWrite ) throws IOException {
            if ( rejectStatementCloseBeforeSend && Request.parseFrom( message ).getTypeCase() == TypeCase.CLOSE_STATEMENT_REQUEST ) {
                return false;
            }
            return Transport.super.trySendMessage( message, deadlineNanos, beginWrite );
        }


        @Override
        public void sendMessage( byte[] message ) throws IOException {
            Request request = Request.parseFrom( message );
            TypeCase type = request.getTypeCase();
            requests.add( type );
            if ( holdUpdatesAndTheirFollowingClose && type == TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST
                    && request.getExecuteUnparameterizedStatementRequest().getStatement().equals( "UPDATE items SET x = 1" ) ) {
                blockedUpdateId = request.getId();
                blockedUpdateSent.countDown();
                return;
            }
            if ( holdUpdatesAndTheirFollowingClose && type == TypeCase.CLOSE_STATEMENT_REQUEST && blockedUpdateId != 0 ) {
                queuedCloseId = request.getId();
                queuedCloseSent.countDown();
                return;
            }

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
                case CONNECTION_REQUEST:
                    ConnectionResponse.Builder connection = ConnectionResponse.newBuilder().setIsCompatible( true )
                            .setMajorApiVersion( 2 ).setMinorApiVersion( 5 ).setSupportsGeneratedKeys( true );
                    if ( advertiseQueryTimeout ) {
                        connection.setUnknownFields( UnknownFieldSet.newBuilder()
                                .addField( QUERY_TIMEOUT_EXTENSION, UnknownFieldSet.Field.newBuilder().addVarint( 1 ).build() ).build() );
                    }
                    response.setConnectionResponse( connection );
                    break;
                case SET_CLIENT_INFO_PROPERTIES_REQUEST:
                    boundaryEventsRequested = "true".equals( request.getSetClientInfoPropertiesRequest().getPropertiesMap().get( "transactionBoundaryEvents" ) );
                    response.setSetClientInfoPropertiesResponse( ClientInfoPropertiesResponse.getDefaultInstance() );
                    break;
                case CONNECTION_PROPERTIES_UPDATE_REQUEST:
                    response.setConnectionPropertiesUpdateResponse( ConnectionPropertiesUpdateResponse.getDefaultInstance() );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST:
                case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST:
                    StatementBatchResponse.Builder batch = StatementBatchResponse.newBuilder().setBatchId( 41 ).addScalars( 1 );
                    if ( returnGeneratedKeys ) {
                        batch.addGeneratedKeys( BatchGeneratedKeys.newBuilder().setStatementIndex( 0 ).setKeys( insertResult().getGeneratedKeys() ) );
                    }
                    response.setStatementBatchResponse( batch );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                    if ( sendImplicitBoundary ) {
                        sendImplicitBoundary = false;
                        Response.Builder boundary = Response.newBuilder().setId( request.getId() );
                        if ( implicitBoundaryFails ) {
                            boundary.setRollbackResponse( RollbackResponse.getDefaultInstance() );
                            response.setErrorResponse( ErrorResponse.newBuilder().setMessage( "DDL rolled back" )
                                    .setDetails( ErrorDetails.newBuilder().setState( "42P16" ) ) );
                        } else {
                            boundary.setCommitResponse( CommitResponse.getDefaultInstance() );
                            response.setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 )
                                    .setResult( StatementResult.newBuilder().setScalar( 1 ) ) );
                        }
                        responses.add( boundary.build().toByteArray() );
                    } else {
                        StatementResult result = request.getExecuteUnparameterizedStatementRequest().hasGeneratedKeys() ? insertResult() : queryResult();
                        response.setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 ).setResult( result ) );
                    }
                    break;
                case PREPARE_INDEXED_STATEMENT_REQUEST:
                    response.setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 41 ) );
                    break;
                case EXECUTE_INDEXED_STATEMENT_REQUEST:
                    response.setStatementResult( returnGeneratedKeys ? insertResult() : queryResult() );
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
                case COMMIT_REQUEST:
                    response.setCommitResponse( CommitResponse.getDefaultInstance() );
                    break;
                case ROLLBACK_REQUEST:
                    response.setRollbackResponse( RollbackResponse.getDefaultInstance() );
                    break;
                case DISCONNECT_REQUEST:
                    response.setDisconnectResponse( DisconnectResponse.getDefaultInstance() );
                    break;
                default:
                    throw new AssertionError( "Unexpected request: " + type );
            }
            responses.add( response.build().toByteArray() );
        }


        private static StatementResult insertResult() {
            return StatementResult.newBuilder().setScalar( 1 )
                    .setGeneratedKeys( Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.newBuilder()
                            .addColumnMeta( ColumnMeta.newBuilder().setColumnName( "id" )
                                    .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.BIGINT ) ) )
                            .addRows( Row.newBuilder().addValues( ProtoValue.newBuilder().setLong( ProtoLong.newBuilder().setLong( 1 ) ) ) ) ) )
                    .build();
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

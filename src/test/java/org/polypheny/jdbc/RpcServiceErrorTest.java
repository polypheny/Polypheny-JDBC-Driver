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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.ProtocolException;
import java.nio.channels.ClosedByInterruptException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLTimeoutException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.types.PolyBlob;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CloseResultResponse;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.ConnectionCheckResponse;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Namespace;
import org.polypheny.prism.NamespacesResponse;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Timeout(5)
class RpcServiceErrorTest {

    private static final String LEGACY_MESSAGE = "  Statement failed (SQLSTATE 23505)\nDetails follow.  ";

    private final BlockingQueue<byte[]> incoming = new LinkedBlockingQueue<>();
    private final byte[] endOfStream = new byte[0];
    private final EOFException eof = new EOFException( "Connection ended before a response" );
    private RequestHandler requestHandler;
    private RpcService rpc;
    private int transportCloses;


    @FunctionalInterface
    private interface RequestHandler {

        void send( Request request ) throws IOException;

    }


    @BeforeEach
    void setUp() {
        rpc = new RpcService( new Transport() {
            @Override
            public void sendMessage( byte[] message ) throws IOException {
                requestHandler.send( Request.parseFrom( message ) );
            }

            @Override
            public byte[] receiveMessage() throws IOException {
                try {
                    byte[] message = incoming.take();
                    if ( message == endOfStream ) {
                        throw eof;
                    }
                    return message;
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new IOException( e );
                }
            }

            @Override
            public void close() {
                transportCloses++;
                incoming.add( endOfStream );
            }
        } );
    }


    @AfterEach
    void tearDown() {
        rpc.close();
    }


    @ParameterizedTest
    @ValueSource(ints = { 0, 1 })
    void connectionCheckUsesTheNetworkDeadlineAndClosesOutstandingObjects( int validationSeconds ) throws SQLException {
        Connection connection = createConnection();
        connection.setNetworkTimeout( Runnable::run, 100 );
        Statement open = connection.createStatement();
        replyTo( TypeCase.CONNECTION_CHECK_REQUEST );

        assertFalse( connection.isValid( validationSeconds ) );
        assertTrue( connection.isClosed() );
        assertTrue( open.isClosed() );
        assertEquals( 1, transportCloses );
    }


    @Test
    void shorterValidationDeadlineLeavesTheConnectionAvailable() throws SQLException {
        Connection connection = createConnection();
        connection.setNetworkTimeout( Runnable::run, 3000 );
        replyTo( TypeCase.CONNECTION_CHECK_REQUEST );

        assertFalse( connection.isValid( 1 ) );
        assertFalse( connection.isClosed() );
        replyTo( TypeCase.CONNECTION_CHECK_REQUEST,
                Response.newBuilder().setLast( true ).setConnectionCheckResponse( ConnectionCheckResponse.getDefaultInstance() ) );
        assertTrue( connection.isValid( 1 ) );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void metadataNetworkDeadlineClosesTheConnectionAndPreservesSQLException( boolean nestedRequest ) throws SQLException {
        Connection connection = createConnection();
        connection.setNetworkTimeout( Runnable::run, 100 );
        Statement open = connection.createStatement();
        requestHandler = request -> {
            if ( nestedRequest && request.hasNamespacesRequest() ) {
                incoming.add( Response.newBuilder().setId( request.getId() ).setLast( true )
                        .setNamespacesResponse( NamespacesResponse.newBuilder().addNamespaces( Namespace.newBuilder().setNamespaceName( "public" ) ) )
                        .build().toByteArray() );
            } else {
                assertEquals( nestedRequest ? TypeCase.ENTITIES_REQUEST : TypeCase.DEFAULT_NAMESPACE_REQUEST, request.getTypeCase() );
            }
        };

        SQLException failure = nestedRequest
                ? assertThrows( SQLException.class, () -> connection.getMetaData().getTables( null, "public", "%", null ) )
                : assertThrows( SQLException.class, () -> connection.getMetaData().getCatalogs() );

        assertEquals( "08003", failure.getSQLState() );
        assertInstanceOf( TimeoutException.class, failure.getCause() );
        assertTrue( connection.isClosed() );
        assertTrue( open.isClosed() );
    }


    @ParameterizedTest
    @MethodSource("structuredDetails")
    void preparedStatementReadsOptionalFieldsIndependently( ErrorDetails details, String state, int code ) throws Exception {
        replyTo( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST, errorResponse( details ) );
        Connection connection = createConnection();

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> connection.prepareStatement( "SELECT * FROM missing_table" ) );
        assertServerError( failure, state, code );
    }


    private static Stream<Arguments> structuredDetails() {
        return Stream.of(
                Arguments.of( null, "UNSPECIFIED", -1 ),
                Arguments.of( ErrorDetails.getDefaultInstance(), "UNSPECIFIED", -1 ),
                Arguments.of( ErrorDetails.newBuilder().setState( "XX000" ).build(), "XX000", -1 ),
                Arguments.of( ErrorDetails.newBuilder().setState( "Z9A01" ).setErrorCode( -123 ).build(), "Z9A01", -123 ),
                Arguments.of( ErrorDetails.newBuilder().setErrorCode( 17 ).build(), "UNSPECIFIED", 17 ),
                Arguments.of( ErrorDetails.newBuilder().setState( "42P01" ).setErrorCode( 0 ).setMessage( "Nested details message" ).build(), "42P01", 0 )
        );
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void batchExceptionCarriesDiagnosticAndReportedCounts( boolean prepared, boolean emptyProgress ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            long[] expected = emptyProgress ? new long[0] : new long[]{ 0, 1L + Integer.MAX_VALUE };
            ErrorDetails details = ErrorDetails.newBuilder().setState( "23505" ).setErrorCode( 71 ).build();
            replyToBatch( prepared, batchResponse( 42, false, expected ), errorResponse( details ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            addBatchEntry( statement );
            BatchUpdateException failure = assertThrows( BatchUpdateException.class, statement::executeLargeBatch );
            assertEquals( LEGACY_MESSAGE, failure.getMessage() );
            assertEquals( "23505", failure.getSQLState() );
            assertEquals( 71, failure.getErrorCode() );
            assertArrayEquals( expected, failure.getLargeUpdateCounts() );
            assertServerError( assertInstanceOf( PrismInterfaceServiceException.class, failure.getCause() ), "23505", 71 );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void preservesLegacyBatchErrorsWithoutClaimingProgress( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            replyToBatch( prepared, errorResponse( null ) );
            addBatchEntry( statement );
            BatchUpdateException failure = assertThrows( BatchUpdateException.class, statement::executeBatch );
            assertEquals( LEGACY_MESSAGE, failure.getMessage() );
            assertEquals( "UNSPECIFIED", failure.getSQLState() );
            assertEquals( -1, failure.getErrorCode() );
            assertNull( failure.getUpdateCounts() );
            assertNull( failure.getLargeUpdateCounts() );
            assertServerError( assertInstanceOf( PrismInterfaceServiceException.class, failure.getCause() ), "UNSPECIFIED", -1 );
        }
    }


    @Test
    void rejectsZeroIdInRawBatchAcknowledgment() throws Exception {
        try ( Statement statement = createBatchStatement( false ) ) {
            replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, batchResponse( 0, false ) );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rejectsMoreCountsThanRequestedEntries( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            replyToBatch( prepared, batchResponse( 42, true, 1, 1 ) );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @ParameterizedTest
    @ValueSource(ints = { -1, 0, 1 })
    void rejectsInvalidOrDuplicateGeneratedKeyIndexes( int index ) throws Exception {
        try ( Statement statement = createBatchStatement( true ) ) {
            Response.Builder response = batchResponse( 42, true, 1 );
            response.getStatementBatchResponseBuilder().addGeneratedKeys( BatchGeneratedKeys.getDefaultInstance() )
                    .addGeneratedKeys( BatchGeneratedKeys.newBuilder().setStatementIndex( index ) );
            replyToBatch( true, response );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void rejectsUnexpectedBatchResponseTypeAfterProgress( boolean prepared, boolean last ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            replyToBatch( prepared, batchResponse( 42, false, 1 ), Response.newBuilder().setLast( last )
                    .setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );

            replyToBatch( prepared, batchResponse( 42, true, 1 ) );
            addBatchEntry( statement );
            assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rejectsUnexpectedBatchResponseId( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            replyToBatch( prepared, batchResponse( 43, true, 1 ) );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rejectsMissingCountsInFinalBatchResponse( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            replyToBatch( prepared, batchResponse( 42, true ) );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true" })
    void batchDeadlineClosesTheConnectionAndItsStatements( boolean prepared, boolean queryDeadline ) throws Exception {
        try ( Connection connection = createConnection(); Statement previousStatement = connection.createStatement() ) {
            connection.setNetworkTimeout( Runnable::run, queryDeadline ? 3000 : 50 );
            Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();
            replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, Response.newBuilder().setLast( true )
                    .setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 )
                            .setResult( StatementResult.newBuilder().setFrame( frame ) ) ) );
            try ( ResultSet previousResult = previousStatement.executeQuery( "SELECT 1" );
                    Statement statement = createBatchStatement( connection, prepared ) ) {
                if ( queryDeadline ) {
                    statement.setQueryTimeout( 1 );
                }
                replyToBatch( prepared, batchResponse( 42, false, prepared ? new long[]{ 1 } : new long[0] ) );
                addBatchEntry( statement );

                SQLException failure = assertThrows( SQLException.class, statement::executeBatch );
                assertInstanceOf( TimeoutException.class, failure.getCause() );
                if ( queryDeadline ) {
                    assertInstanceOf( SQLTimeoutException.class, failure );
                } else {
                    assertInstanceOf( PrismInterfaceServiceException.class, failure );
                    assertEquals( "08003", failure.getSQLState() );
                }
                assertTrue( connection.isClosed() );
                assertEquals( 1, transportCloses );
                assertFalse( connection.isValid( 0 ) );
                assertTrue( statement.isClosed() );
                assertTrue( previousStatement.isClosed() );
                assertTrue( previousResult.isClosed() );
                assertThrows( SQLException.class, connection::createStatement );
                assertThrows( SQLException.class, statement::executeBatch );
            }
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void interruptedBatchClosesTheConnectionAndPreservesTheInterrupt( boolean prepared, boolean whileSending ) throws Exception {
        try ( Connection connection = createConnection(); Statement statement = createBatchStatement( connection, prepared ) ) {
            CountDownLatch sent = new CountDownLatch( 1 );
            requestHandler = request -> {
                assertEquals( prepared ? TypeCase.EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST : TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST,
                        request.getTypeCase() );
                sent.countDown();
                if ( whileSending ) {
                    Thread.currentThread().interrupt();
                    throw new ClosedByInterruptException();
                }
            };
            addBatchEntry( statement );
            FutureTask<SQLException> execution = new FutureTask<>( () -> {
                SQLException failure = assertThrows( SQLException.class, statement::executeBatch );
                assertTrue( Thread.currentThread().isInterrupted() );
                return failure;
            } );
            Thread worker = new Thread( execution, "interrupted-batch-test" );
            worker.setDaemon( true );
            worker.start();
            try {
                assertTrue( sent.await( 1, TimeUnit.SECONDS ) );
                if ( !whileSending ) {
                    worker.interrupt();
                }
                SQLException failure = execution.get( 1, TimeUnit.SECONDS );
                Class<? extends Exception> expectedCause = whileSending ? ClosedByInterruptException.class : InterruptedException.class;
                assertInstanceOf( expectedCause, failure.getCause() );
                assertTrue( connection.isClosed() );
                assertTrue( statement.isClosed() );
                assertFalse( connection.isValid( 0 ) );
                assertEquals( 1, transportCloses );
            } finally {
                worker.interrupt();
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void reportsUnknownPerCommandCountsForPreparedBatchAggregate( boolean large ) throws Exception {
        try ( Statement statement = createBatchStatement( true ) ) {
            replyToBatch( true, batchResponse( 42, true, 3 ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            if ( large ) {
                assertArrayEquals( new long[]{ Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO }, statement.executeLargeBatch() );
            } else {
                assertArrayEquals( new int[]{ Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO }, statement.executeBatch() );
            }
        }
    }


    @Test
    void preservesExactCountsForPreparedBatches() throws Exception {
        try ( Statement statement = createBatchStatement( true ) ) {
            replyToBatch( true, batchResponse( 42, true, 7 ) );
            addBatchEntry( statement );
            assertArrayEquals( new int[]{ 7 }, statement.executeBatch() );

            replyToBatch( true, batchResponse( 42, true, 0, 2 ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            assertArrayEquals( new int[]{ 0, 2 }, statement.executeBatch() );

            replyToBatch( true, batchResponse( 42, true, 0, Long.MAX_VALUE ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            assertArrayEquals( new long[]{ 0, Long.MAX_VALUE }, statement.executeLargeBatch() );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void rejectsShortTerminalBatchCounts( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            Response.Builder response = prepared ? batchResponse( 42, true, 1, 1 ) : batchResponse( 42, true, 1 );
            replyToBatch( prepared, response );
            addBatchEntry( statement );
            addBatchEntry( statement );
            if ( prepared ) {
                addBatchEntry( statement );
            }
            assertBatchProtocolFailure( statement );
        }
    }


    @Test
    void rejectsPreparedAggregateCountWithGeneratedKeys() throws Exception {
        Frame keys = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();
        Response.Builder response = batchResponse( 42, true, 2 );
        response.getStatementBatchResponseBuilder().addGeneratedKeys( BatchGeneratedKeys.newBuilder().setStatementIndex( 0 ).setKeys( keys ) );
        try ( Statement statement = createBatchStatement( true ) ) {
            replyToBatch( true, response );
            addBatchEntry( statement );
            addBatchEntry( statement );
            assertBatchProtocolFailure( statement );
        }
    }


    @Test
    void acceptsZeroCountsForAnEmptyBatch() throws Exception {
        replyToBatch( true, batchResponse( 42, true ) );
        ExecuteIndexedStatementBatchRequest prepared = ExecuteIndexedStatementBatchRequest.newBuilder().setStatementId( 42 ).build();
        assertEquals( 0, rpc.executeIndexedStatementBatch( prepared, 1000 ).getScalarsCount() );

        replyToBatch( false, batchResponse( 42, true ) );
        List<Integer> batchIds = new ArrayList<>();
        ExecuteUnparameterizedStatementBatchRequest raw = ExecuteUnparameterizedStatementBatchRequest.getDefaultInstance();
        assertEquals( 0, rpc.executeUnparameterizedStatementBatch( raw, 1000, batchIds::add ).getScalarsCount() );
        assertEquals( Collections.singletonList( 42 ), batchIds );
    }


    @Test
    void doesNotAcceptAnIdFromAnInvalidInitialBatchPayload() {
        replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST, batchResponse( 42, true, 1, 1 ) );
        ExecuteUnparameterizedStatementBatchRequest request = ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                .addStatements( ExecuteUnparameterizedStatementRequest.getDefaultInstance() ).build();
        List<Integer> batchIds = new ArrayList<>();
        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> rpc.executeUnparameterizedStatementBatch( request, 1000, batchIds::add ) );
        assertInstanceOf( ProtocolException.class, failure.getCause() );
        assertEquals( Collections.emptyList(), batchIds );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void previousResultCleanupFailureKeepsPrismExceptionType( boolean prepared ) throws Exception {
        try ( Statement statement = createBatchStatement( prepared ) ) {
            TypeCase cleanup;
            if ( prepared ) {
                Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();
                replyTo( TypeCase.EXECUTE_INDEXED_STATEMENT_REQUEST, Response.newBuilder().setLast( true )
                        .setStatementResult( StatementResult.newBuilder().setFrame( frame ) ) );
                ((PreparedStatement) statement).executeQuery();
                cleanup = TypeCase.CLOSE_RESULT_REQUEST;
            } else {
                replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, Response.newBuilder().setLast( true )
                        .setStatementResponse( StatementResponse.newBuilder().setStatementId( 42 )
                                .setResult( StatementResult.newBuilder().setScalar( 1 ) ) ) );
                statement.executeUpdate( "INSERT INTO example DEFAULT VALUES" );
                cleanup = TypeCase.CLOSE_STATEMENT_REQUEST;
            }
            addBatchEntry( statement );
            ErrorDetails details = ErrorDetails.newBuilder().setState( "40001" ).setErrorCode( 7 ).build();
            requestHandler = request -> {
                assertEquals( cleanup, request.getTypeCase() );
                incoming.add( errorResponse( details ).setId( request.getId() ).build().toByteArray() );
            };
            try {
                PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, statement::executeBatch );
                assertServerError( failure, "40001", 7 );
            } finally {
                replyTo( cleanup, Response.newBuilder().setLast( true ).setCloseResultResponse( CloseResultResponse.getDefaultInstance() ) );
            }
            assertArrayEquals( new int[0], statement.executeBatch() );
        }
    }


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "true, true" })
    void expiredDeadlineStopsPreparedSerializationAndLeavesTheConnectionOpen( boolean batch, boolean laterBatchEntry ) throws Exception {
        Connection connection = createConnection();
        replyTo( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST, Response.newBuilder().setLast( true )
                .setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 42 )
                        .addParameterMetas( ParameterMeta.newBuilder().setTypeName( "FILE" ) )
                        .addParameterMetas( ParameterMeta.newBuilder().setTypeName( "FILE" ) ) ) );
        try ( PreparedStatement statement = connection.prepareStatement( "INSERT INTO example VALUES (?, ?) RETURNING id" ) ) {
            statement.setBlob( 1, new PolyBlob( new byte[]{ 1 } ) );
            statement.setBlob( 2, new PolyBlob( new byte[]{ 1 } ) );
            Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();
            Response.Builder successfulInsert = Response.newBuilder().setLast( true )
                    .setStatementResult( StatementResult.newBuilder().setFrame( frame ) );
            replyTo( TypeCase.EXECUTE_INDEXED_STATEMENT_REQUEST, successfulInsert );
            ResultSet previousResult = statement.executeQuery();

            AtomicBoolean serializationStarted = new AtomicBoolean();
            statement.setBlob( 1, new PolyBlob( new byte[]{ 2 } ) {
                @Override
                public InputStream getBinaryStream() throws SQLException {
                    serializationStarted.set( true );
                    try {
                        TimeUnit.MILLISECONDS.sleep( 650 );
                    } catch ( InterruptedException e ) {
                        Thread.currentThread().interrupt();
                        throw new SQLException( e );
                    }
                    return super.getBinaryStream();
                }
            } );
            statement.setQueryTimeout( 1 );
            PolyBlob unread = new PolyBlob( new byte[]{ 3 } ) {
                @Override
                public InputStream getBinaryStream() {
                    throw new AssertionError( "Read a later BLOB after expiry" );
                }
            };
            if ( laterBatchEntry ) {
                statement.addBatch();
                statement.setBlob( 1, unread );
            } else {
                statement.setBlob( 2, unread );
            }
            if ( batch ) {
                statement.addBatch();
            }
            AtomicInteger submitted = new AtomicInteger();
            requestHandler = request -> {
                Response.Builder response;
                switch ( request.getTypeCase() ) {
                    case CLOSE_RESULT_REQUEST:
                        try {
                            TimeUnit.MILLISECONDS.sleep( 500 );
                        } catch ( InterruptedException e ) {
                            Thread.currentThread().interrupt();
                            throw new IOException( e );
                        }
                        response = Response.newBuilder().setLast( true ).setCloseResultResponse( CloseResultResponse.getDefaultInstance() );
                        break;
                    case CLOSE_STATEMENT_REQUEST:
                        response = Response.newBuilder().setLast( true ).setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() );
                        break;
                    case EXECUTE_INDEXED_STATEMENT_REQUEST:
                        submitted.incrementAndGet();
                        response = successfulInsert;
                        break;
                    case EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST:
                        submitted.incrementAndGet();
                        response = batchResponse( 42, true, 1 );
                        break;
                    default:
                        throw new AssertionError( "Unexpected request: " + request.getTypeCase() );
                }
                incoming.add( response.setId( request.getId() ).build().toByteArray() );
            };
            SQLException failure = batch
                    ? assertThrows( SQLTimeoutException.class, statement::executeBatch )
                    : assertThrows( SQLTimeoutException.class, statement::executeQuery );
            assertInstanceOf( TimeoutException.class, failure.getCause() );
            assertTrue( serializationStarted.get() );
            assertEquals( 0, submitted.get() );
            assertFalse( connection.isClosed() );
            assertFalse( statement.isClosed() );
            assertTrue( previousResult.isClosed() );
            assertEquals( 0, transportCloses );

            statement.setQueryTimeout( 0 );
            statement.setBlob( 1, new PolyBlob( new byte[]{ 1 } ) );
            statement.setBlob( 2, new PolyBlob( new byte[]{ 1 } ) );
            if ( batch ) {
                statement.addBatch();
                assertArrayEquals( new int[]{ 1 }, statement.executeBatch() );
            } else {
                statement.executeQuery().close();
            }
            assertEquals( 1, submitted.get() );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void generatedKeyPrepareDistinguishesNetworkTimeoutFromServerSqlstate( boolean serverError ) throws Exception {
        Connection connection = createConnection( true );
        connection.setNetworkTimeout( Runnable::run, 200 );
        try ( Statement previouslyOpen = connection.createStatement() ) {
            requestHandler = request -> {
                assertEquals( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST, request.getTypeCase() );
                assertTrue( request.getPrepareIndexedStatementRequest().hasGeneratedKeys() );
                if ( serverError ) {
                    ErrorDetails details = ErrorDetails.newBuilder().setState( "08006" ).setErrorCode( 37 ).build();
                    incoming.add( errorResponse( details ).setId( request.getId() ).build().toByteArray() );
                }
            };
            PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                    () -> connection.prepareStatement( "INSERT INTO example DEFAULT VALUES", Statement.RETURN_GENERATED_KEYS ) );
            if ( serverError ) {
                assertServerError( failure, "08006", 37 );
                assertFalse( connection.isClosed() );
                assertFalse( previouslyOpen.isClosed() );
                assertEquals( 0, transportCloses );
            } else {
                assertEquals( "08003", failure.getSQLState() );
                assertInstanceOf( TimeoutException.class, failure.getCause() );
                assertTrue( connection.isClosed() );
                assertTrue( previouslyOpen.isClosed() );
                assertEquals( 1, transportCloses );
            }
        }
    }


    private static void assertBatchProtocolFailure( Statement statement ) {
        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, statement::executeLargeBatch );
        assertInstanceOf( ProtocolException.class, failure.getCause() );
        assertNull( failure.getNextException() );
    }


    private Statement createBatchStatement( boolean prepared ) throws SQLException {
        return createBatchStatement( createConnection(), prepared );
    }


    private Statement createBatchStatement( Connection connection, boolean prepared ) throws SQLException {
        if ( prepared ) {
            replyTo( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST, Response.newBuilder().setLast( true )
                    .setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 42 ) ) );
            return connection.prepareStatement( "INSERT INTO example DEFAULT VALUES" );
        }
        return connection.createStatement();
    }


    private static void addBatchEntry( Statement statement ) throws SQLException {
        if ( statement instanceof PreparedStatement ) {
            ((PreparedStatement) statement).addBatch();
        } else {
            statement.addBatch( "INSERT INTO example DEFAULT VALUES" );
        }
    }


    private void replyToBatch( boolean prepared, Response.Builder... responses ) {
        List<Response.Builder> frames = new ArrayList<>();
        if ( !prepared ) {
            frames.add( batchResponse( 42, false ) );
        }
        Collections.addAll( frames, responses );
        TypeCase request = prepared ? TypeCase.EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST : TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST;
        replyTo( request, frames.toArray( new Response.Builder[0] ) );
    }


    private static Response.Builder batchResponse( int batchId, boolean last, long... counts ) {
        StatementBatchResponse.Builder batch = StatementBatchResponse.newBuilder().setBatchId( batchId );
        for ( long count : counts ) {
            batch.addScalars( count );
        }
        return Response.newBuilder().setLast( last ).setStatementBatchResponse( batch );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void statementCallbackPropagatesServerErrorsBeforeAndAfterAResult( boolean sendPartialResult ) throws Exception {
        ErrorDetails details = ErrorDetails.newBuilder().setState( "23505" ).setErrorCode( 0 ).setMessage( "Nested details message" ).build();
        Response.Builder failureResponse = errorResponse( details );
        if ( sendPartialResult ) {
            Response.Builder partial = Response.newBuilder()
                    .setStatementResponse( StatementResponse.newBuilder().setStatementId( 42 )
                            .setResult( StatementResult.newBuilder().setScalar( 1 ) ) );
            replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, partial, failureResponse );
        } else {
            replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, failureResponse );
        }

        try ( Statement statement = createConnection().createStatement() ) {
            PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                    () -> statement.execute( "INSERT INTO example VALUES (1)" ) );
            assertServerError( failure, "23505", 0 );
        }
    }


    @ParameterizedTest(name = "{0} with SQLSTATE [{1}]")
    @MethodSource("invalidStates")
    void ignoresInvalidSqlStateAndPreservesExplicitZero( TypeCase request, String invalidState ) throws SQLException {
        ErrorDetails details = ErrorDetails.newBuilder().setState( invalidState ).setErrorCode( 0 )
                .setMessage( "Nested details message" ).build();

        PrismInterfaceServiceException failure = failJdbcOperation( request, errorResponse( details ) );
        assertServerError( failure, "UNSPECIFIED", 0 );
    }


    private static Stream<Arguments> invalidStates() {
        return Stream.of( "", "23", "42p01", "42-01", "42P01 " )
                .flatMap( state -> Stream.of(
                        Arguments.of( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST, state ),
                        Arguments.of( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, state ) ) );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void treatsNonFinalErrorResponseAsProtocolFailure( boolean callback ) throws SQLException {
        ErrorDetails details = ErrorDetails.newBuilder().setState( "23505" ).setErrorCode( 0 ).build();
        TypeCase request = callback ? TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST : TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST;

        PrismInterfaceServiceException failure = failJdbcOperation( request, errorResponse( details ).setLast( false ) );
        assertUnspecified( failure );
        assertEquals( LEGACY_MESSAGE, failure.getMessage() );
        assertInstanceOf( ProtocolException.class, failure.getCause() );
    }


    private PrismInterfaceServiceException failJdbcOperation( TypeCase request, Response.Builder response ) throws SQLException {
        replyTo( request, response );
        Connection connection = createConnection();
        if ( request == TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST ) {
            return assertThrows( PrismInterfaceServiceException.class, () -> connection.prepareStatement( "SELECT 1" ) );
        }
        try ( Statement statement = connection.createStatement() ) {
            return assertThrows( PrismInterfaceServiceException.class, () -> statement.execute( "SELECT 1" ) );
        }
    }


    @Test
    void preservesTransportSendFailureAsCause() throws Exception {
        IOException transportFailure = new IOException( "Unable to send request" );
        requestHandler = request -> {
            throw transportFailure;
        };

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> createConnection().prepareStatement( "SELECT 1" ) );
        assertEquals( "08006", failure.getSQLState() );
        assertSame( transportFailure, failure.getCause() );
    }


    @Test
    void preservesSynchronousTransportReadFailureAsCause() throws Exception {
        endStreamOnRequest( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST );

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> createConnection().prepareStatement( "SELECT 1" ) );
        assertEquals( "08006", failure.getSQLState() );
        assertSame( eof, failure.getCause() );
    }


    @Test
    void preservesCallbackTransportReadFailureAsCause() throws Exception {
        endStreamOnRequest( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST );

        try ( Statement statement = createConnection().createStatement() ) {
            PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                    () -> statement.execute( "SELECT 1" ) );
            assertEquals( "08006", failure.getSQLState() );
            assertSame( eof, failure.getCause() );
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void failedRegistrationClosesTransport( boolean duringCapabilityNegotiation ) throws Exception {
        ConnectionString target = new ConnectionString( "jdbc:polypheny:http://localhost/", new Properties() );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        ErrorDetails details = ErrorDetails.newBuilder().setState( "08004" ).setErrorCode( 17 ).build();
        requestHandler = request -> {
            if ( duringCapabilityNegotiation && request.hasConnectionRequest() ) {
                incoming.add( Response.newBuilder().setId( request.getId() ).setLast( true ).setConnectionResponse(
                        ConnectionResponse.newBuilder().setIsCompatible( true ).setSupportsGeneratedKeys( true ) ).build().toByteArray() );
                return;
            }
            assertEquals( duringCapabilityNegotiation ? TypeCase.SET_CLIENT_INFO_PROPERTIES_REQUEST : TypeCase.CONNECTION_REQUEST, request.getTypeCase() );
            incoming.add( errorResponse( details ).setId( request.getId() ).build().toByteArray() );
        };

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, () -> client.register( properties, 1000 ) );
        assertServerError( failure, "08004", 17 );
        assertTrue( client.isClosed() );
        assertEquals( 1, transportCloses );
    }


    @Test
    void connectionCloseDisposesRemainingStatementsAfterServerError() throws Exception {
        Connection connection = createConnection();
        Statement statement = connection.createStatement();
        Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();
        replyTo( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST, Response.newBuilder().setLast( true ).setStatementResponse(
                StatementResponse.newBuilder().setStatementId( 41 ).setResult( StatementResult.newBuilder().setFrame( frame ) ) ) );
        ResultSet result = statement.executeQuery( "SELECT 1" );
        Statement remaining = connection.createStatement();
        replyTo( TypeCase.CLOSE_RESULT_REQUEST, errorResponse( ErrorDetails.newBuilder().setState( "XX000" ).setErrorCode( 17 ).build() ) );

        assertServerError( assertThrows( PrismInterfaceServiceException.class, connection::close ), "XX000", 17 );
        assertTrue( connection.isClosed() );
        assertTrue( statement.isClosed() );
        assertTrue( result.isClosed() );
        assertTrue( remaining.isClosed() );
        assertEquals( 1, transportCloses );
    }


    @Test
    void preservesServerErrorInClientInfoException() throws Exception {
        ErrorDetails details = ErrorDetails.newBuilder().setState( "XX000" ).setErrorCode( 0 ).build();
        replyTo( TypeCase.CLIENT_INFO_PROPERTIES_REQUEST, errorResponse( details ) );

        SQLClientInfoException failure = assertThrows( SQLClientInfoException.class, createConnection()::getClientInfo );
        assertEquals( LEGACY_MESSAGE, failure.getMessage() );
        assertEquals( "XX000", failure.getSQLState() );
        assertEquals( 0, failure.getErrorCode() );
        assertServerError( assertInstanceOf( PrismInterfaceServiceException.class, failure.getCause() ), "XX000", 0 );
    }


    private Connection createConnection() throws SQLException {
        return createConnection( false );
    }


    private Connection createConnection( boolean generatedKeys ) throws SQLException {
        ConnectionString target = new ConnectionString( "jdbc:polypheny:http://localhost/", new Properties() );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        if ( generatedKeys ) {
            requestHandler = request -> {
                Response.Builder response = Response.newBuilder().setId( request.getId() ).setLast( true );
                if ( request.hasConnectionRequest() ) {
                    response.setConnectionResponse( ConnectionResponse.newBuilder().setIsCompatible( true ).setSupportsGeneratedKeys( true ) );
                } else {
                    assertEquals( TypeCase.SET_CLIENT_INFO_PROPERTIES_REQUEST, request.getTypeCase() );
                    response.setSetClientInfoPropertiesResponse( ClientInfoPropertiesResponse.getDefaultInstance() );
                }
                incoming.add( response.build().toByteArray() );
            };
            client.register( properties, 1000 );
        }
        return new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
    }


    private static Response.Builder errorResponse( ErrorDetails details ) {
        ErrorResponse.Builder error = ErrorResponse.newBuilder().setMessage( LEGACY_MESSAGE );
        if ( details != null ) {
            error.setDetails( details );
        }
        return Response.newBuilder().setLast( true ).setErrorResponse( error );
    }


    private void replyTo( TypeCase expectedRequest, Response.Builder... replies ) {
        requestHandler = request -> {
            if ( request.hasCloseStatementRequest() ) {
                incoming.add( Response.newBuilder().setId( request.getId() ).setLast( true )
                        .setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() ).build().toByteArray() );
                return;
            }
            assertEquals( expectedRequest, request.getTypeCase() );
            for ( Response.Builder reply : replies ) {
                incoming.add( reply.setId( request.getId() ).build().toByteArray() );
            }
        };
    }


    private void endStreamOnRequest( TypeCase expectedRequest ) {
        requestHandler = request -> {
            assertEquals( expectedRequest, request.getTypeCase() );
            incoming.add( endOfStream );
        };
    }


    private static void assertServerError( PrismInterfaceServiceException failure, String state, int code ) {
        assertEquals( LEGACY_MESSAGE, failure.getMessage() );
        assertEquals( state, failure.getSQLState() );
        assertEquals( code, failure.getErrorCode() );
        assertNull( failure.getCause() );
        assertNull( failure.getNextException() );
    }


    private static void assertUnspecified( PrismInterfaceServiceException failure ) {
        assertEquals( "UNSPECIFIED", failure.getSQLState() );
        assertEquals( -1, failure.getErrorCode() );
        assertNull( failure.getNextException() );
    }

}

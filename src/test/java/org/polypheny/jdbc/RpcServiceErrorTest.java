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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.EOFException;
import java.io.IOException;
import java.net.ProtocolException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
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
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
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
class RpcServiceErrorTest {

    private static final String LEGACY_MESSAGE = "  Statement failed (SQLSTATE 23505)\nDetails follow.  ";

    private final BlockingQueue<byte[]> incoming = new LinkedBlockingQueue<>();
    private final byte[] endOfStream = new byte[0];
    private final EOFException eof = new EOFException( "Connection ended before a response" );
    private RequestHandler requestHandler;
    private RpcService rpc;


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
                incoming.add( endOfStream );
            }
        } );
    }


    @AfterEach
    void tearDown() {
        rpc.close();
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


    @Test
    void acceptsLegacyAggregateCountForPreparedBatch() throws Exception {
        try ( Statement statement = createBatchStatement( true ) ) {
            replyToBatch( true, batchResponse( 42, true, 2 ) );
            addBatchEntry( statement );
            addBatchEntry( statement );
            assertArrayEquals( new long[]{ 2 }, statement.executeLargeBatch() );
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


    private static void assertBatchProtocolFailure( Statement statement ) {
        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, statement::executeLargeBatch );
        assertInstanceOf( ProtocolException.class, failure.getCause() );
        assertNull( failure.getNextException() );
    }


    private Statement createBatchStatement( boolean prepared ) throws SQLException {
        Connection connection = createConnection();
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
        assertUnspecified( failure );
        assertSame( transportFailure, failure.getCause() );
    }


    @Test
    void preservesSynchronousTransportReadFailureChain() throws Exception {
        endStreamOnRequest( TypeCase.PREPARE_INDEXED_STATEMENT_REQUEST );

        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                () -> createConnection().prepareStatement( "SELECT 1" ) );
        assertUnspecified( failure );
        assertSame( eof, assertInstanceOf( ExecutionException.class, failure.getCause() ).getCause() );
    }


    @Test
    void preservesCallbackTransportReadFailureAsCause() throws Exception {
        endStreamOnRequest( TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST );

        try ( Statement statement = createConnection().createStatement() ) {
            PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                    () -> statement.execute( "SELECT 1" ) );
            assertUnspecified( failure );
            assertSame( eof, failure.getCause() );
        }
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
        ConnectionString target = new ConnectionString( "jdbc:polypheny:http://localhost/", new Properties() );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        return new PolyConnection( new PolyphenyConnectionProperties( target, client ), new PolyphenyDatabaseMetadata( client, target ) );
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

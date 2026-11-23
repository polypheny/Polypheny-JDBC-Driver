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

import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
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
        checkBatchCleanup( cleanup, false );
    }


    @ParameterizedTest
    @EnumSource( Cleanup.class )
    void preservesServerErrorFromCleanupBeforeBatch( Cleanup cleanup ) throws Exception {
        checkBatchCleanup( cleanup, true );
    }


    private static void checkBatchCleanup( Cleanup cleanup, boolean serverError ) throws Exception {
        CleanupTransport transport = new CleanupTransport( cleanup.requestType, serverError );
        RpcService rpc = new RpcService( transport );
        try {
            PrismInterfaceClient client = new PrismInterfaceClient( rpc );
            ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
            PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
            try ( Connection connection = new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
                    Statement statement = cleanup == Cleanup.PREPARED_RESULT ? connection.prepareStatement( SELECT ) : connection.createStatement() ) {
                connection.setNetworkTimeout( Runnable::run, 150 );
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
                PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, statement::executeBatch );
                assertEquals( 1, transport.requests.size() - requestsBeforeCleanup );
                assertEquals( cleanup.requestType, transport.requests.get( requestsBeforeCleanup ) );
                if ( serverError ) {
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
                    assertEquals( "08003", failure.getSQLState() );
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
        private final boolean serverError;
        private boolean interceptedCleanup;


        private CleanupTransport( TypeCase cleanupType, boolean serverError ) {
            this.cleanupType = cleanupType;
            this.serverError = serverError;
        }


        @Override
        public void sendMessage( byte[] message ) throws IOException {
            Request request = Request.parseFrom( message );
            TypeCase type = request.getTypeCase();
            requests.add( type );
            Response.Builder response = Response.newBuilder().setId( request.getId() ).setLast( true );
            if ( type == cleanupType && !interceptedCleanup ) {
                interceptedCleanup = true;
                if ( serverError ) {
                    response.setErrorResponse( ErrorResponse.newBuilder().setMessage( "Cannot close previous resource" )
                            .setDetails( ErrorDetails.newBuilder().setState( "26000" ).setErrorCode( 73 ) ) );
                    responses.add( response.build().toByteArray() );
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
                    response.setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() );
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

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.DisconnectResponse;
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;


@Timeout(5)
class StatementDeadlineTest {

    private static final String INSERT = "INSERT INTO items DEFAULT VALUES";


    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true", "true, true" })
    void closesConnectionWhenASingleGeneratedKeyInsertDoesNotFinish( boolean prepared, boolean queryOnly ) throws Exception {
        InsertTransport transport = new InsertTransport( false );
        RpcService rpc = new RpcService( transport );
        try {
            try ( Connection connection = connection( rpc );
                    Statement statement = prepared ? connection.prepareStatement( INSERT, Statement.RETURN_GENERATED_KEYS ) : connection.createStatement() ) {
                if ( queryOnly ) {
                    statement.setQueryTimeout( 1 );
                } else {
                    connection.setNetworkTimeout( Runnable::run, 120 );
                }

                SQLException failure = prepared
                        ? assertThrows( SQLException.class, ((PreparedStatement) statement)::executeUpdate )
                        : assertThrows( SQLException.class, () -> statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );

                assertInstanceOf( TimeoutException.class, failure.getCause() );
                if ( queryOnly ) {
                    assertInstanceOf( SQLTimeoutException.class, failure );
                } else {
                    assertEquals( "08003", failure.getSQLState() );
                }
                assertEquals( 1, transport.inserts.get() );
                assertEquals( 1, transport.closes.get() );
                assertTrue( connection.isClosed() );
                assertTrue( statement.isClosed() );
                assertFalse( connection.isValid( 0 ) );
            }
        } finally {
            rpc.close();
        }
    }


    @Test
    void preservesAServerConnectionSqlstateWithoutAbortingTheTransport() throws Exception {
        InsertTransport transport = new InsertTransport( true );
        RpcService rpc = new RpcService( transport );
        try ( Connection connection = connection( rpc ); Statement statement = connection.createStatement() ) {
            SQLException failure = assertThrows( SQLException.class, () -> statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );

            assertEquals( "08006", failure.getSQLState() );
            assertEquals( "Remote adapter connection failed", failure.getMessage() );
            assertFalse( connection.isClosed() );
            assertFalse( statement.isClosed() );
            assertEquals( 0, transport.closes.get() );
        } finally {
            rpc.close();
        }
    }


    private static Connection connection( RpcService rpc ) throws SQLException {
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        client.register( properties, 1000 );
        return new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
    }


    private static final class InsertTransport implements Transport {

        private final byte[] endOfStream = new byte[0];
        private final BlockingQueue<byte[]> responses = new LinkedBlockingQueue<>();
        private final AtomicInteger inserts = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final boolean serverError;


        private InsertTransport( boolean serverError ) {
            this.serverError = serverError;
        }


        @Override
        public void sendMessage( byte[] bytes ) throws IOException {
            Request request = Request.parseFrom( bytes );
            Response.Builder response = Response.newBuilder().setId( request.getId() ).setLast( true );
            switch ( request.getTypeCase() ) {
                case CONNECTION_REQUEST:
                    response.setConnectionResponse( ConnectionResponse.newBuilder().setIsCompatible( true ).setSupportsGeneratedKeys( true ) );
                    break;
                case SET_CLIENT_INFO_PROPERTIES_REQUEST:
                    response.setSetClientInfoPropertiesResponse( ClientInfoPropertiesResponse.getDefaultInstance() );
                    break;
                case CONNECTION_PROPERTIES_UPDATE_REQUEST:
                    response.setConnectionPropertiesUpdateResponse( ConnectionPropertiesUpdateResponse.getDefaultInstance() );
                    break;
                case PREPARE_INDEXED_STATEMENT_REQUEST:
                    assertTrue( request.getPrepareIndexedStatementRequest().hasGeneratedKeys() );
                    response.setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 41 ) );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                    assertTrue( request.getExecuteUnparameterizedStatementRequest().hasGeneratedKeys() );
                    inserts.incrementAndGet();
                    if ( serverError ) {
                        response.setErrorResponse( ErrorResponse.newBuilder()
                                .setMessage( "Remote adapter connection failed" )
                                .setDetails( ErrorDetails.newBuilder().setState( "08006" ) ) );
                    } else {
                        response.setLast( false ).setStatementResponse( StatementResponse.newBuilder().setStatementId( 41 ) );
                    }
                    break;
                case EXECUTE_INDEXED_STATEMENT_REQUEST:
                    inserts.incrementAndGet();
                    return;
                case CLOSE_STATEMENT_REQUEST:
                    response.setCloseStatementResponse( CloseStatementResponse.getDefaultInstance() );
                    break;
                case DISCONNECT_REQUEST:
                    response.setDisconnectResponse( DisconnectResponse.getDefaultInstance() );
                    break;
                default:
                    throw new AssertionError( "Unexpected request: " + request.getTypeCase() );
            }
            responses.add( response.build().toByteArray() );
        }


        @Override
        public byte[] receiveMessage() throws IOException {
            try {
                byte[] response = responses.take();
                if ( response == endOfStream ) {
                    throw new EOFException();
                }
                return response;
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

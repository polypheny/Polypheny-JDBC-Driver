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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;


@Timeout(5)
class BatchConnectionLossTest {

    private static final String INSERT = "INSERT INTO items DEFAULT VALUES";
    private static final String SELECT = "SELECT 1";


    @ParameterizedTest(name = "prepared={0}, reset={1}, large={2}")
    @CsvSource({ "false, false, false", "false, true, true", "true, false, true", "true, true, false" })
    void remoteLossDuringBatchClosesTrackedJdbcResources( boolean prepared, boolean reset, boolean large ) throws Exception {
        IOException remoteFailure = reset ? new SocketException( "Connection reset" ) : new EOFException( "End of stream" );
        TypeCase batchRequestType = prepared ? TypeCase.EXECUTE_INDEXED_STATEMENT_BATCH_REQUEST : TypeCase.EXECUTE_UNPARAMETERIZED_STATEMENT_BATCH_REQUEST;
        BatchTransport transport = new BatchTransport( batchRequestType, remoteFailure );
        RpcService rpc = new RpcService( transport );
        try {
            PrismInterfaceClient client = new PrismInterfaceClient( rpc );
            ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost/public" );
            PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
            int forwardOnly = ResultSet.TYPE_FORWARD_ONLY;
            int readOnly = ResultSet.CONCUR_READ_ONLY;
            int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
            try ( Connection connection = new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
                    Statement reader = connection.createStatement( forwardOnly, readOnly );
                    PreparedStatement preparedReader = connection.prepareStatement( SELECT, forwardOnly, readOnly );
                    Statement batch = prepared ? connection.prepareStatement( INSERT, forwardOnly, readOnly, holdability )
                            : connection.createStatement( forwardOnly, readOnly, holdability );
                    ResultSet rows = reader.executeQuery( SELECT );
                    ResultSet preparedRows = preparedReader.executeQuery() ) {
                if ( prepared ) {
                    ((PreparedStatement) batch).addBatch();
                } else {
                    batch.addBatch( INSERT );
                }

                int sentBeforeBatch = transport.requests.get();
                SQLException failure = large ? assertThrows( SQLException.class, batch::executeLargeBatch )
                        : assertThrows( SQLException.class, batch::executeBatch );
                assertEquals( sentBeforeBatch + 1, transport.requests.get() );
                assertEquals( "08006", failure.getSQLState() );
                assertSame( remoteFailure, failure.getCause() );

                assertTrue( connection.isClosed() );
                assertFalse( connection.isValid( 0 ) );
                assertTrue( batch.isClosed() );
                assertTrue( reader.isClosed() );
                assertTrue( preparedReader.isClosed() );
                assertTrue( rows.isClosed() );
                assertTrue( preparedRows.isClosed() );
                assertThrows( SQLException.class, rows::next );
                assertThrows( SQLException.class, preparedRows::next );
                assertEquals( 1, transport.closes.get() );
            }
        } finally {
            rpc.close();
        }
    }


    private static final class BatchTransport implements Transport {

        private final BlockingQueue<byte[]> incoming = new LinkedBlockingQueue<>();
        private final byte[] endOfStream = new byte[0];
        private final AtomicInteger requests = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();
        private final TypeCase batchRequestType;
        private final IOException remoteFailure;


        private BatchTransport( TypeCase batchRequestType, IOException remoteFailure ) {
            this.batchRequestType = batchRequestType;
            this.remoteFailure = remoteFailure;
        }


        @Override
        public void sendMessage( byte[] message ) throws IOException {
            Request request = Request.parseFrom( message );
            TypeCase type = request.getTypeCase();
            requests.incrementAndGet();
            if ( type == batchRequestType ) {
                incoming.add( endOfStream );
                return;
            }

            Response.Builder response = Response.newBuilder().setId( request.getId() ).setLast( true );
            switch ( type ) {
                case PREPARE_INDEXED_STATEMENT_REQUEST:
                    response.setPreparedStatementSignature( PreparedStatementSignature.newBuilder().setStatementId( 42 ) );
                    break;
                case EXECUTE_UNPARAMETERIZED_STATEMENT_REQUEST:
                    response.setStatementResponse( StatementResponse.newBuilder().setStatementId( 43 ).setResult( queryResult() ) );
                    break;
                case EXECUTE_INDEXED_STATEMENT_REQUEST:
                    response.setStatementResult( queryResult() );
                    break;
                default:
                    throw new AssertionError( "Unexpected request: " + type );
            }
            incoming.add( response.build().toByteArray() );
        }


        private static StatementResult queryResult() {
            return StatementResult.newBuilder()
                    .setFrame( Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ) )
                    .build();
        }


        @Override
        public byte[] receiveMessage() throws IOException {
            try {
                byte[] response = incoming.take();
                if ( response == endOfStream ) {
                    throw remoteFailure;
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
            incoming.add( endOfStream );
        }

    }

}

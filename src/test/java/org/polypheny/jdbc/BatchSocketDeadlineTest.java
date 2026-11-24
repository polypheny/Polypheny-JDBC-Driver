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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;

@Timeout(10)
class BatchSocketDeadlineTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void networkDeadlineClosesConnectionWhenPeerStopsReading( boolean generatedKeyPrepare ) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool( 2 );
        CountDownLatch releasePeer = new CountDownLatch( 1 );
        try ( ServerSocket server = new ServerSocket() ) {
            server.setReceiveBufferSize( 4096 );
            server.bind( new InetSocketAddress( "127.0.0.1", 0 ) );
            server.setSoTimeout( 3000 );
            Future<?> peer = executor.submit( () -> {
                try ( Socket socket = server.accept() ) {
                    acknowledgeConnectionSettings( socket, generatedKeyPrepare );
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
                if ( generatedKeyPrepare ) {
                    client.register( properties, 3000 );
                }
                PolyConnection connection = new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
                connection.setNetworkTimeout( Runnable::run, 500 );
                Statement statement = connection.createStatement();
                char[] payload = new char[8 * 1024 * 1024];
                Arrays.fill( payload, 'x' );
                String sql = "INSERT INTO probe VALUES ('" + new String( payload ) + "')";
                Future<SQLException> result;
                if ( generatedKeyPrepare ) {
                    result = executor.submit( () -> assertThrows( SQLException.class,
                            () -> connection.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) );
                } else {
                    statement.addBatch( sql );
                    result = executor.submit( () -> assertThrows( SQLException.class, statement::executeBatch ) );
                }
                assertTrue( writingRequest.await( 3, TimeUnit.SECONDS ) );
                SQLException failure = result.get( 3, TimeUnit.SECONDS );
                assertEquals( "08003", failure.getSQLState() );
                assertInstanceOf( TimeoutException.class, failure.getCause() );
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


    private static void acknowledgeConnectionSettings( Socket socket, boolean generatedKeyPrepare ) throws IOException {
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
        if ( generatedKeyPrepare ) {
            Request connect = readRequest( input, Request.TypeCase.CONNECTION_REQUEST );
            writeResponse( output, connect, Response.newBuilder().setConnectionResponse(
                    ConnectionResponse.newBuilder().setIsCompatible( true ).setSupportsGeneratedKeys( true ) ) );
            Request clientInfo = readRequest( input, Request.TypeCase.SET_CLIENT_INFO_PROPERTIES_REQUEST );
            writeResponse( output, clientInfo, Response.newBuilder().setSetClientInfoPropertiesResponse( ClientInfoPropertiesResponse.getDefaultInstance() ) );
        }
        Request settings = readRequest( input, Request.TypeCase.CONNECTION_PROPERTIES_UPDATE_REQUEST );
        writeResponse( output, settings, Response.newBuilder().setConnectionPropertiesUpdateResponse( ConnectionPropertiesUpdateResponse.getDefaultInstance() ) );
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

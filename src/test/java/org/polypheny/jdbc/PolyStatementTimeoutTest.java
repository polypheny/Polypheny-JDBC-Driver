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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.prism.Request;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;

class PolyStatementTimeoutTest {

    @ParameterizedTest
    @CsvSource({ "false, false", "true, false", "false, true" })
    @Timeout(5)
    void closesTheConnectionWhenCypherDoesNotReturnAnExecutionResult( boolean acknowledgeStatement, boolean interrupted ) throws Exception {
        byte[] endOfStream = new byte[0];
        BlockingQueue<byte[]> incoming = new LinkedBlockingQueue<>();
        AtomicInteger sends = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        Transport transport = new Transport() {
            @Override
            public void sendMessage( byte[] message ) throws IOException {
                sends.incrementAndGet();
                Request execution = Request.parseFrom( message );
                assertTrue( execution.hasExecuteUnparameterizedStatementRequest() );
                if ( acknowledgeStatement ) {
                    incoming.add( Response.newBuilder().setId( execution.getId() )
                            .setStatementResponse( StatementResponse.newBuilder().setStatementId( 42 ) ).build().toByteArray() );
                }
                if ( interrupted ) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public byte[] receiveMessage() throws IOException {
                try {
                    byte[] message = incoming.take();
                    if ( message == endOfStream ) {
                        throw new EOFException();
                    }
                    return message;
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
        };

        RpcService rpc = new RpcService( transport );
        try {
            PolyphenyConnectionProperties settings = mock( PolyphenyConnectionProperties.class );
            when( settings.getPrismInterfaceClient() ).thenReturn( new PrismInterfaceClient( rpc ) );
            when( settings.getNetworkTimeout() ).thenReturn( 500 );
            try ( PolyConnection connection = new PolyConnection( settings, mock( PolyphenyDatabaseMetadata.class ) );
                    PolyStatement statement = connection.createPolyStatement() ) {
                PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class,
                        () -> statement.execute( "graph", "cypher", "MATCH (n) RETURN n" ) );
                Class<? extends Exception> failureType = interrupted ? InterruptedException.class : TimeoutException.class;
                assertInstanceOf( failureType, failure.getCause() );
                assertEquals( interrupted, Thread.currentThread().isInterrupted() );
                assertEquals( acknowledgeStatement ? 42 : -1, statement.getStatementId() );
                assertTrue( connection.isClosed() );

                assertThrows( PrismInterfaceServiceException.class,
                        () -> statement.execute( "graph", "cypher", "MATCH (n) RETURN n" ) );
                assertEquals( 1, sends.get() );
                assertEquals( 1, closes.get() );
            }
        } finally {
            if ( interrupted ) {
                Thread.interrupted();
            }
            rpc.close();
        }
    }

}

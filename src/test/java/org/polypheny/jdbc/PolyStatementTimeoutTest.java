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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.EOFException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
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
        Transport transport = mock( Transport.class );
        when( transport.receiveMessage() ).thenAnswer( invocation -> {
            byte[] message = incoming.take();
            if ( message == endOfStream ) {
                throw new EOFException();
            }
            return message;
        } );
        doAnswer( invocation -> {
            incoming.add( endOfStream );
            return null;
        } ).when( transport ).close();
        doAnswer( invocation -> {
            Request execution = Request.parseFrom( (byte[]) invocation.getArgument( 0 ) );
            assertTrue( execution.hasExecuteUnparameterizedStatementRequest() );
            if ( acknowledgeStatement ) {
                incoming.add( Response.newBuilder().setId( execution.getId() )
                        .setStatementResponse( StatementResponse.newBuilder().setStatementId( 42 ) ).build().toByteArray() );
            }
            if ( interrupted ) {
                Thread.currentThread().interrupt();
            }
            return null;
        } ).when( transport ).sendMessage( any( byte[].class ) );

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
                verify( transport ).sendMessage( any( byte[].class ) );
                verify( transport ).close();
            }
        } finally {
            if ( interrupted ) {
                Thread.interrupted();
            }
            rpc.close();
        }
    }

}

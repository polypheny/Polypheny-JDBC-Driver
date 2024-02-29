/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.ConnectionResponse;
import org.polypheny.db.protointerface.proto.DisconnectRequest;
import org.polypheny.db.protointerface.proto.DisconnectResponse;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.Request;
import org.polypheny.db.protointerface.proto.Response;
import org.polypheny.db.protointerface.proto.StatementResponse;
import org.polypheny.jdbc.utils.CallbackQueue;

@Slf4j
public class RpcService {

    private final AtomicLong idCounter = new AtomicLong( 1 );
    private final InputStream in;
    private final OutputStream out;
    private final Thread service;
    private final Map<Long, Consumer<Response>> callbacks = new ConcurrentHashMap<>();


    RpcService( InputStream in, OutputStream out ) {
        this.in = in;
        this.out = out;
        this.service = new Thread( this::readResponses );
        this.service.start();
    }


    private Request.Builder newMessage() {
        long id = idCounter.getAndIncrement();
        return Request.newBuilder().setId( id );
    }


    private void sendMessage( Request req ) throws IOException {
        byte[] b = req.toByteArray();
        ByteBuffer bb = ByteBuffer.allocate( 8 );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( b.length );
        out.write( bb.array() );
        out.write( b );
    }


    private Response receiveMessage() throws IOException {
        byte[] b = in.readNBytes( 8 );
        if ( b.length != 8 ) {
            if ( b.length == 0 ) { // EOF
                throw new EOFException();
            }
            throw new IOException( "short read" );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        byte[] msg = in.readNBytes( (int) length );
        return Response.parseFrom( msg );
    }


    private void readResponses() {
        while ( true ) {
            try {
                Response resp = receiveMessage();
                if ( resp.getId() == 0 ) {
                    throw new RuntimeException( "Invalid message id" );
                }
                Consumer<Response> c = callbacks.get( resp.getId() );
                if ( c == null ) {
                    log.info( "No callback for response of type " + resp.getTypeCase() );
                    continue;
                }
                if ( resp.getLast() ) {
                    callbacks.remove( resp.getId() );
                }
                c.accept( resp );
            } catch ( IOException e ) { // Communicate this to ProtoInterfaceClient
                throw new RuntimeException( e );
            }
        }
    }


    private static void completeOrThrow( CompletableFuture<Response> f, Response r ) {
        if ( r.hasErrorResponse() ) {
            f.completeExceptionally( new ProtoInterfaceServiceException( r.getErrorResponse().getMessage() ) );
        } else {
            f.complete( r );
        }
    }


    public ConnectionResponse connect( ConnectionRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionRequest( msg );
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), r -> completeOrThrow( f, r ) );
            sendMessage( req.build() );
            if ( timeout == 0 ) {
                return f.get().getConnectionResponse();
            } else {
                return f.get( timeout, TimeUnit.MILLISECONDS ).getConnectionResponse();
            }
        } catch ( TimeoutException | IOException | InterruptedException | ExecutionException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }


    public DisconnectResponse disconnect( DisconnectRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDisconnectRequest( msg );
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), r -> completeOrThrow( f, r ) );
            sendMessage( req.build() );
            if ( timeout == 0 ) {
                return f.get().getDisconnectResponse();
            } else {
                return f.get( timeout, TimeUnit.MILLISECONDS ).getDisconnectResponse();
            }
        } catch ( IOException | ExecutionException | InterruptedException | TimeoutException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }


    public void executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest msg, CallbackQueue<StatementResponse> callback ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementRequest( msg );
        try {
            callbacks.put( req.getId(), v -> {
                if ( v.hasErrorResponse() ) {
                    callback.onError( new ProtoInterfaceServiceException( v.getErrorResponse().getMessage() ) );
                } else {
                    callback.onNext( v.getStatementResponse() );
                    if ( v.getLast() ) {
                        callback.onCompleted();
                    }
                }
            } );
            sendMessage( req.build() );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }


    public CloseStatementResponse closeStatement( CloseStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), r -> completeOrThrow( f, r ) );
            sendMessage( req.build() );
            if ( timeout == 0 ) {
                return f.get().getCloseStatementResponse();
            } else {
                return f.get( timeout, TimeUnit.MILLISECONDS ).getCloseStatementResponse();
            }
        } catch ( ExecutionException | InterruptedException | TimeoutException | IOException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }

}

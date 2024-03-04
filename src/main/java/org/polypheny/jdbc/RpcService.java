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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMetaRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMetaResponse;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CloseStatementResponse;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.CommitResponse;
import org.polypheny.db.protointerface.proto.ConnectionCheckRequest;
import org.polypheny.db.protointerface.proto.ConnectionCheckResponse;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateRequest;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateResponse;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.ConnectionResponse;
import org.polypheny.db.protointerface.proto.DatabasesRequest;
import org.polypheny.db.protointerface.proto.DatabasesResponse;
import org.polypheny.db.protointerface.proto.DbmsVersionRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionResponse;
import org.polypheny.db.protointerface.proto.DisconnectRequest;
import org.polypheny.db.protointerface.proto.DisconnectResponse;
import org.polypheny.db.protointerface.proto.EntitiesRequest;
import org.polypheny.db.protointerface.proto.EntitiesResponse;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.FunctionsRequest;
import org.polypheny.db.protointerface.proto.FunctionsResponse;
import org.polypheny.db.protointerface.proto.MetaStringResponse;
import org.polypheny.db.protointerface.proto.NamespacesRequest;
import org.polypheny.db.protointerface.proto.NamespacesResponse;
import org.polypheny.db.protointerface.proto.PrepareStatementRequest;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.ProceduresRequest;
import org.polypheny.db.protointerface.proto.ProceduresResponse;
import org.polypheny.db.protointerface.proto.Request;
import org.polypheny.db.protointerface.proto.Response;
import org.polypheny.db.protointerface.proto.RollbackRequest;
import org.polypheny.db.protointerface.proto.RollbackResponse;
import org.polypheny.db.protointerface.proto.SqlKeywordsRequest;
import org.polypheny.db.protointerface.proto.SqlNumericFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlStringFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlSystemFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlTimeDateFunctionsRequest;
import org.polypheny.db.protointerface.proto.StatementBatchResponse;
import org.polypheny.db.protointerface.proto.StatementResponse;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.TableTypesRequest;
import org.polypheny.db.protointerface.proto.TableTypesResponse;
import org.polypheny.db.protointerface.proto.TypesRequest;
import org.polypheny.db.protointerface.proto.TypesResponse;
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


    private Response completeSynchronously( Request.Builder req, int timeout ) throws ProtoInterfaceServiceException {
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), r -> completeOrThrow( f, r ) );
            sendMessage( req.build() );
            if ( timeout == 0 ) {
                return f.get();
            } else {
                return f.get( timeout, TimeUnit.MILLISECONDS );
            }
        } catch ( IOException | ExecutionException | InterruptedException | TimeoutException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }


    public ConnectionResponse connect( ConnectionRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionResponse();
    }


    public ConnectionCheckResponse checkConnection( ConnectionCheckRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionCheckRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionCheckResponse();
    }


    public ConnectionPropertiesUpdateResponse updateConnectionProperties( ConnectionPropertiesUpdateRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionPropertiesUpdateRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionPropertiesUpdateResponse();
    }


    public DbmsVersionResponse getDbmsVersion( DbmsVersionRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDbmsVersionRequest( msg );
        return completeSynchronously( req, timeout ).getDbmsVersionResponse();
    }


    public DatabasesResponse getDatabases( DatabasesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDatabasesRequest( msg );
        return completeSynchronously( req, timeout ).getDatabasesResponse();
    }


    public ClientInfoPropertyMetaResponse getClientInfoPropertiesMetas( ClientInfoPropertyMetaRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setClientInfoPropertyMetaRequest( msg );
        return completeSynchronously( req, timeout ).getClientInfoPropertyMetaResponse();
    }


    public TableTypesResponse getTableTypes( TableTypesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTableTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTableTypesResponse();
    }


    public TypesResponse getTypes( TypesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTypesResponse();
    }


    public ProceduresResponse searchProcedures( ProceduresRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setProceduresRequest( msg );
        return completeSynchronously( req, timeout ).getProceduresResponse();
    }


    public FunctionsResponse searchFunctions( FunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getFunctionsResponse();
    }


    public NamespacesResponse searchNamespaces( NamespacesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setNamespacesRequest( msg );
        return completeSynchronously( req, timeout ).getNamespacesResponse();
    }


    public EntitiesResponse searchEntities( EntitiesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setEntitiesRequest( msg );
        return completeSynchronously( req, timeout ).getEntitiesResponse();
    }


    public MetaStringResponse getSqlStringFunctions( SqlStringFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlStringFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlStringFunctionsResponse();
    }


    public MetaStringResponse getSqlSystemFunctions( SqlSystemFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlSystemFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlSystemFunctionsResponse();
    }


    public MetaStringResponse getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlTimeDateFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlTimeDateFunctionsResponse();
    }


    public MetaStringResponse getSqlNumericFunctions( SqlNumericFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlNumericFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlNumericFunctionsResponse();
    }


    public MetaStringResponse getSqlKeywords( SqlKeywordsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlKeywordsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlKeywordsResponse();
    }


    public DisconnectResponse disconnect( DisconnectRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDisconnectRequest( msg );
        return completeSynchronously( req, timeout ).getDisconnectResponse();
    }


    public CommitResponse commit( CommitRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCommitRequest( msg );
        return completeSynchronously( req, timeout ).getCommitResponse();
    }


    public RollbackResponse rollback( RollbackRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setRollbackRequest( msg );
        return completeSynchronously( req, timeout ).getRollbackResponse();
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


    public void executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest msg, CallbackQueue<StatementBatchResponse> callback ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementBatchRequest( msg );
        try {
            callbacks.put( req.getId(), v -> {
                if ( v.hasErrorResponse() ) {
                    callback.onError( new ProtoInterfaceServiceException( v.getErrorResponse().getMessage() ) );
                } else {
                    callback.onNext( v.getStatementBatchResponse() );
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


    public PreparedStatementSignature prepareIndexedStatement( PrepareStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setPrepareIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getPreparedStatementSignature();
    }


    public StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getStatementResult();
    }


    public CloseStatementResponse closeStatement( CloseStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        return completeSynchronously( req, timeout ).getCloseStatementResponse();
    }

}

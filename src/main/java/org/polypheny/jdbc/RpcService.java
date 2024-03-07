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
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.protointerface.proto.ClientInfoProperties;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesResponse;
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
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.FetchRequest;
import org.polypheny.db.protointerface.proto.Frame;
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
import org.polypheny.db.protointerface.proto.UserDefinedTypesRequest;
import org.polypheny.db.protointerface.proto.UserDefinedTypesResponse;
import org.polypheny.jdbc.utils.CallbackQueue;

@Slf4j
public class RpcService {

    private final AtomicLong idCounter = new AtomicLong( 1 );
    private final Transport con;
    private final Thread service;
    private boolean closed = false;
    private IOException error = null;
    private final Map<Long, Consumer<Response>> callbacks = new ConcurrentHashMap<>();


    RpcService( Transport con ) {
        this.con = con;
        this.service = new Thread( this::readResponses );
        this.service.start();
    }


    void close() {
        closed = true;
    }


    private Request.Builder newMessage() {
        long id = idCounter.getAndIncrement();
        return Request.newBuilder().setId( id );
    }


    private void sendMessage( Request req ) throws IOException {
        if ( this.error != null ) {
            synchronized ( this ) {
                IOException e = this.error;
                this.error = null;
                throw e;
            }
        }
        if ( this.closed ) {
            throw new IOException( "Connection is closed" );
        }
        con.sendMessage( req.toByteArray() );
    }


    private Response receiveMessage() throws IOException {
        return Response.parseFrom( con.receiveMessage() );
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
                this.closed = true;
                if ( e instanceof EOFException && closed ) {
                    // Nothing to worry about
                    return;
                } else {
                    // This will cause the exception to be thrown when the next call is made
                    // TODO: Is this good enough, or should the program be alerted sooner?
                    this.error = e;
                    throw new RuntimeException( e );
                }
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


    ConnectionResponse connect( ConnectionRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionResponse();
    }


    ConnectionCheckResponse checkConnection( ConnectionCheckRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionCheckRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionCheckResponse();
    }


    ConnectionPropertiesUpdateResponse updateConnectionProperties( ConnectionPropertiesUpdateRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionPropertiesUpdateRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionPropertiesUpdateResponse();
    }


    DbmsVersionResponse getDbmsVersion( DbmsVersionRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDbmsVersionRequest( msg );
        return completeSynchronously( req, timeout ).getDbmsVersionResponse();
    }


    DatabasesResponse getDatabases( DatabasesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDatabasesRequest( msg );
        return completeSynchronously( req, timeout ).getDatabasesResponse();
    }


    ClientInfoPropertyMetaResponse getClientInfoPropertiesMetas( ClientInfoPropertyMetaRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setClientInfoPropertyMetaRequest( msg );
        return completeSynchronously( req, timeout ).getClientInfoPropertyMetaResponse();
    }


    TableTypesResponse getTableTypes( TableTypesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTableTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTableTypesResponse();
    }


    UserDefinedTypesResponse getUserDefinedTypes( UserDefinedTypesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setUserDefinedTypesRequest( msg );
        return completeSynchronously( req, timeout ).getUserDefinedTypesResponse();
    }


    TypesResponse getTypes( TypesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTypesResponse();
    }


    ProceduresResponse searchProcedures( ProceduresRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setProceduresRequest( msg );
        return completeSynchronously( req, timeout ).getProceduresResponse();
    }


    FunctionsResponse searchFunctions( FunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getFunctionsResponse();
    }


    NamespacesResponse searchNamespaces( NamespacesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setNamespacesRequest( msg );
        return completeSynchronously( req, timeout ).getNamespacesResponse();
    }


    EntitiesResponse searchEntities( EntitiesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setEntitiesRequest( msg );
        return completeSynchronously( req, timeout ).getEntitiesResponse();
    }


    ClientInfoPropertiesResponse setClientInfoProperties( ClientInfoProperties msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSetClientInfoPropertiesRequest( msg );
        return completeSynchronously( req, timeout ).getSetClientInfoPropertiesResponse();
    }


    ClientInfoProperties getClientInfoProperties( ClientInfoPropertiesRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setClientInfoPropertiesRequest( msg );
        return completeSynchronously( req, timeout ).getClientInfoPropertiesResponse();
    }


    MetaStringResponse getSqlStringFunctions( SqlStringFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlStringFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlStringFunctionsResponse();
    }


    MetaStringResponse getSqlSystemFunctions( SqlSystemFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlSystemFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlSystemFunctionsResponse();
    }


    MetaStringResponse getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlTimeDateFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlTimeDateFunctionsResponse();
    }


    MetaStringResponse getSqlNumericFunctions( SqlNumericFunctionsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlNumericFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlNumericFunctionsResponse();
    }


    MetaStringResponse getSqlKeywords( SqlKeywordsRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlKeywordsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlKeywordsResponse();
    }


    DisconnectResponse disconnect( DisconnectRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDisconnectRequest( msg );
        return completeSynchronously( req, timeout ).getDisconnectResponse();
    }


    CommitResponse commit( CommitRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCommitRequest( msg );
        return completeSynchronously( req, timeout ).getCommitResponse();
    }


    RollbackResponse rollback( RollbackRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setRollbackRequest( msg );
        return completeSynchronously( req, timeout ).getRollbackResponse();
    }


    void executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest msg, CallbackQueue<StatementResponse> callback ) throws ProtoInterfaceServiceException {
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


    void executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest msg, CallbackQueue<StatementBatchResponse> callback ) throws ProtoInterfaceServiceException {
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


    PreparedStatementSignature prepareIndexedStatement( PrepareStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setPrepareIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getPreparedStatementSignature();
    }


    StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getStatementResult();
    }


    StatementBatchResponse executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementBatchRequest( msg );
        return completeSynchronously( req, timeout ).getStatementBatchResponse();
    }


    Frame fetchResult( FetchRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFetchRequest( msg );
        return completeSynchronously( req, timeout ).getFrame();
    }


    CloseStatementResponse closeStatement( CloseStatementRequest msg, int timeout ) throws ProtoInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        return completeSynchronously( req, timeout ).getCloseStatementResponse();
    }

}

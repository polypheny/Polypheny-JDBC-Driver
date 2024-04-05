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
import java.nio.channels.ClosedChannelException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.protointerface.proto.ClientInfoProperties;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesResponse;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMetaRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMetaResponse;
import org.polypheny.db.protointerface.proto.CloseResultRequest;
import org.polypheny.db.protointerface.proto.CloseResultResponse;
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
import org.polypheny.db.protointerface.proto.Response.TypeCase;
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
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.utils.CallbackQueue;

@Slf4j
public class RpcService {

    private final AtomicLong idCounter = new AtomicLong( 1 );
    private final Transport con;
    private final Thread service;
    private boolean closed = false;
    private boolean disconnectSent = false;
    private IOException error = null;
    private final Map<Long, CompletableFuture<Response>> callbacks = new ConcurrentHashMap<>();
    private final Map<Long, CallbackQueue<?>> callbackQueues = new ConcurrentHashMap<>();


    RpcService( Transport con ) {
        this.con = con;
        this.service = new Thread( this::readResponses, "PrismInterfaceResponseHandler" );
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
                CompletableFuture<Response> c = callbacks.get( resp.getId() );
                if ( c == null ) {
                    CallbackQueue<?> cq = callbackQueues.get( resp.getId() );
                    if ( cq != null ) {
                        if ( resp.hasErrorResponse() ) {
                            callbackQueues.remove( resp.getId() );
                            cq.onError( new PrismInterfaceServiceException( resp.getErrorResponse().getMessage() ) );
                        } else {
                            cq.onNext( resp );
                            if ( resp.getLast() ) {
                                callbackQueues.remove( resp.getId() );
                                cq.onCompleted();
                            }
                        }
                    } else {
                        log.info( "No callback for response of type " + resp.getTypeCase() );
                    }
                    continue;
                }
                if ( resp.getLast() ) {
                    callbacks.remove( resp.getId() );
                }
                c.complete( resp );
                if ( resp.getTypeCase() == TypeCase.DISCONNECT_RESPONSE ) {
                    throw new EOFException( "Connection closed: Disconnect by client" );
                }
            } catch ( EOFException | ClosedChannelException e ) {
                this.closed = true;
                callbacks.forEach( ( id, c ) -> c.completeExceptionally( e ) );
                callbackQueues.forEach( ( id, cq ) -> cq.onError( e ) );
            } catch ( IOException e ) { // Communicate this to ProtoInterfaceClient
                this.closed = true;
                callbacks.forEach( ( id, c ) -> c.completeExceptionally( e ) );
                callbackQueues.forEach( ( id, cq ) -> cq.onError( e ) );
                /* For Windows */
                if ( e.getMessage().contains( "An existing connection was forcibly closed by the remote host" ) && disconnectSent ) {
                    return;
                }
                // This will cause the exception to be thrown when the next call is made
                // TODO: Is this good enough, or should the program be alerted sooner?
                this.error = e;
                throw new RuntimeException( e );
            } catch ( Throwable t ) {
                this.closed = true;
                callbacks.forEach( ( id, c ) -> c.completeExceptionally( t ) );
                callbackQueues.forEach( ( id, cq ) -> cq.onError( t ) );
                log.error( "Unhandled exception", t );
                throw t;
            }
        }
    }


    private Response waitForCompletion( CompletableFuture<Response> f, int timeout ) throws PrismInterfaceServiceException {
        try {
            if ( timeout == 0 ) {
                return f.get();
            } else {
                return f.get( timeout, TimeUnit.MILLISECONDS );
            }
        } catch ( ExecutionException | InterruptedException | TimeoutException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    private Response completeSynchronously( Request.Builder req, int timeout ) throws PrismInterfaceServiceException {
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), f );
            if ( req.getTypeCase() == Request.TypeCase.DISCONNECT_REQUEST ) {
                disconnectSent = true;
            }
            sendMessage( req.build() );
            Response resp = waitForCompletion( f, timeout );
            if ( resp.hasErrorResponse() ) {
                throw new PrismInterfaceServiceException( resp.getErrorResponse().getMessage() );
            }
            return resp;
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    ConnectionResponse connect( ConnectionRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionResponse();
    }


    ConnectionCheckResponse checkConnection( ConnectionCheckRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionCheckRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionCheckResponse();
    }


    ConnectionPropertiesUpdateResponse updateConnectionProperties( ConnectionPropertiesUpdateRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionPropertiesUpdateRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionPropertiesUpdateResponse();
    }


    DbmsVersionResponse getDbmsVersion( DbmsVersionRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDbmsVersionRequest( msg );
        return completeSynchronously( req, timeout ).getDbmsVersionResponse();
    }


    DatabasesResponse getDatabases( DatabasesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDatabasesRequest( msg );
        return completeSynchronously( req, timeout ).getDatabasesResponse();
    }


    ClientInfoPropertyMetaResponse getClientInfoPropertiesMetas( ClientInfoPropertyMetaRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setClientInfoPropertyMetaRequest( msg );
        return completeSynchronously( req, timeout ).getClientInfoPropertyMetaResponse();
    }


    TableTypesResponse getTableTypes( TableTypesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTableTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTableTypesResponse();
    }


    UserDefinedTypesResponse getUserDefinedTypes( UserDefinedTypesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setUserDefinedTypesRequest( msg );
        return completeSynchronously( req, timeout ).getUserDefinedTypesResponse();
    }


    TypesResponse getTypes( TypesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTypesResponse();
    }


    ProceduresResponse searchProcedures( ProceduresRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setProceduresRequest( msg );
        return completeSynchronously( req, timeout ).getProceduresResponse();
    }


    FunctionsResponse searchFunctions( FunctionsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getFunctionsResponse();
    }


    NamespacesResponse searchNamespaces( NamespacesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setNamespacesRequest( msg );
        return completeSynchronously( req, timeout ).getNamespacesResponse();
    }


    EntitiesResponse searchEntities( EntitiesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setEntitiesRequest( msg );
        return completeSynchronously( req, timeout ).getEntitiesResponse();
    }


    ClientInfoPropertiesResponse setClientInfoProperties( ClientInfoProperties msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSetClientInfoPropertiesRequest( msg );
        return completeSynchronously( req, timeout ).getSetClientInfoPropertiesResponse();
    }


    ClientInfoProperties getClientInfoProperties( ClientInfoPropertiesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setClientInfoPropertiesRequest( msg );
        return completeSynchronously( req, timeout ).getClientInfoPropertiesResponse();
    }


    MetaStringResponse getSqlStringFunctions( SqlStringFunctionsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlStringFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlStringFunctionsResponse();
    }


    MetaStringResponse getSqlSystemFunctions( SqlSystemFunctionsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlSystemFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlSystemFunctionsResponse();
    }


    MetaStringResponse getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlTimeDateFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlTimeDateFunctionsResponse();
    }


    MetaStringResponse getSqlNumericFunctions( SqlNumericFunctionsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlNumericFunctionsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlNumericFunctionsResponse();
    }


    MetaStringResponse getSqlKeywords( SqlKeywordsRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setSqlKeywordsRequest( msg );
        return completeSynchronously( req, timeout ).getSqlKeywordsResponse();
    }


    DisconnectResponse disconnect( DisconnectRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDisconnectRequest( msg );
        return completeSynchronously( req, timeout ).getDisconnectResponse();
    }


    CommitResponse commit( CommitRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCommitRequest( msg );
        return completeSynchronously( req, timeout ).getCommitResponse();
    }


    RollbackResponse rollback( RollbackRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setRollbackRequest( msg );
        return completeSynchronously( req, timeout ).getRollbackResponse();
    }


    void executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest msg, CallbackQueue<StatementResponse> callback ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementRequest( msg );
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessage( req.build() );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    void executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest msg, CallbackQueue<StatementBatchResponse> callback ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementBatchRequest( msg );
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessage( req.build() );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    PreparedStatementSignature prepareIndexedStatement( PrepareStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setPrepareIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getPreparedStatementSignature();
    }


    StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getStatementResult();
    }


    StatementBatchResponse executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementBatchRequest( msg );
        return completeSynchronously( req, timeout ).getStatementBatchResponse();
    }


    Frame fetchResult( FetchRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFetchRequest( msg );
        return completeSynchronously( req, timeout ).getFrame();
    }


    CloseStatementResponse closeStatement( CloseStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        return completeSynchronously( req, timeout ).getCloseStatementResponse();
    }


    CloseResultResponse closeResult( CloseResultRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseResultRequest( msg );
        return completeSynchronously( req, timeout ).getCloseResultResponse();
    }

}

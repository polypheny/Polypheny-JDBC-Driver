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
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
import org.polypheny.prism.ClientInfoPropertiesResponse;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseResultResponse;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.CloseStatementResponse;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.CommitResponse;
import org.polypheny.prism.ConnectionCheckRequest;
import org.polypheny.prism.ConnectionCheckResponse;
import org.polypheny.prism.ConnectionPropertiesUpdateRequest;
import org.polypheny.prism.ConnectionPropertiesUpdateResponse;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.DbmsVersionRequest;
import org.polypheny.prism.DbmsVersionResponse;
import org.polypheny.prism.DefaultNamespaceRequest;
import org.polypheny.prism.DefaultNamespaceResponse;
import org.polypheny.prism.DisconnectRequest;
import org.polypheny.prism.DisconnectResponse;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.EntitiesResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.FunctionsRequest;
import org.polypheny.prism.FunctionsResponse;
import org.polypheny.prism.MetaStringResponse;
import org.polypheny.prism.NamespacesRequest;
import org.polypheny.prism.NamespacesResponse;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProceduresRequest;
import org.polypheny.prism.ProceduresResponse;
import org.polypheny.prism.Request;
import org.polypheny.prism.Request.TypeCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.RollbackResponse;
import org.polypheny.prism.SqlKeywordsRequest;
import org.polypheny.prism.SqlNumericFunctionsRequest;
import org.polypheny.prism.SqlStringFunctionsRequest;
import org.polypheny.prism.SqlSystemFunctionsRequest;
import org.polypheny.prism.SqlTimeDateFunctionsRequest;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.TableTypesRequest;
import org.polypheny.prism.TableTypesResponse;
import org.polypheny.prism.TypesRequest;
import org.polypheny.prism.TypesResponse;
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
        con.close();
        try {
            service.join();
        } catch ( InterruptedException e ) {
            log.warn( "Could not join response handler", e );
        }
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
        try {
            while ( true ) {
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
                        if ( log.isDebugEnabled() ) {
                            log.info( "No callback for response of type {}", resp.getTypeCase() );
                        }
                    }
                    continue;
                }
                if ( resp.getLast() ) {
                    callbacks.remove( resp.getId() );
                }
                c.complete( resp );
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
            if ( req.getTypeCase() == TypeCase.DISCONNECT_REQUEST ) {
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


    DefaultNamespaceResponse defaultNamespaceRequest( DefaultNamespaceRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setDefaultNamespaceRequest( msg );
        return completeSynchronously( req, timeout ).getDefaultNamespaceResponse();
    }


    TableTypesResponse getTableTypes( TableTypesRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setTableTypesRequest( msg );
        return completeSynchronously( req, timeout ).getTableTypesResponse();
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
        try {
            return completeSynchronously( req, timeout ).getDisconnectResponse();
        } catch ( PrismInterfaceServiceException e ) {
            /* For Windows */
            if ( e.getMessage().contains( "An existing connection was forcibly closed by the remote host" ) ) {
                return DisconnectResponse.newBuilder().build();
            }
            throw e;
        }
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

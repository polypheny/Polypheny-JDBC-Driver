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

import java.io.IOException;
import java.net.ProtocolException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.BatchGeneratedKeys;
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
import org.polypheny.prism.ErrorDetails;
import org.polypheny.prism.ErrorResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteNamedStatementRequest;
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

@Slf4j
public class RpcService {

    private static final Pattern SQLSTATE = Pattern.compile( "[0-9A-Z]{5}" );
    private static final ScheduledThreadPoolExecutor WRITE_DEADLINES = createWriteDeadlineExecutor();

    private final AtomicLong idCounter = new AtomicLong( 1 );
    private final AtomicLong transactionGeneration = new AtomicLong();
    private final Map<Long, Response.TypeCase> pendingTransactions = new ConcurrentHashMap<>();
    private volatile boolean transactionBoundaryEvents;
    private final AtomicBoolean transportClosed = new AtomicBoolean();
    private final Transport con;
    private final Thread service;
    private volatile boolean closed = false;
    private final Map<Long, CompletableFuture<Response>> callbacks = new ConcurrentHashMap<>();
    private final Map<Long, CallbackQueue<?>> callbackQueues = new ConcurrentHashMap<>();


    private static ScheduledThreadPoolExecutor createWriteDeadlineExecutor() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor( 1, task -> {
            Thread thread = new Thread( task, "PrismRequestWriteDeadline" );
            thread.setDaemon( true );
            return thread;
        } );
        executor.setRemoveOnCancelPolicy( true );
        executor.setKeepAliveTime( 30, TimeUnit.SECONDS );
        executor.allowCoreThreadTimeOut( true );
        return executor;
    }


    RpcService( Transport con ) {
        this.con = con;
        this.service = new Thread( this::readResponses, "PrismInterfaceResponseHandler" );
        this.service.start();
    }


    void close() {
        abort();
        try {
            service.join();
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
        }
    }


    void abort() {
        closed = true;
        if ( transportClosed.compareAndSet( false, true ) ) {
            con.close();
        }
    }


    boolean isClosed() {
        return closed;
    }


    private PrismInterfaceServiceException connectionFailure( IOException cause ) {
        if ( cause instanceof Transport.WriteInterruptedException ) {
            return PrismInterfaceClient.interruptedBeforeWrite( ((Transport.WriteInterruptedException) cause).getCause() );
        }
        abort();
        return new PrismInterfaceServiceException( PrismInterfaceErrors.CONNECTION_FAILURE, "Prism connection failed.", cause );
    }


    private Request.Builder newMessage() {
        long id = idCounter.getAndIncrement();
        return Request.newBuilder().setId( id );
    }


    private void sendMessage( Request req ) throws IOException {
        if ( this.closed ) {
            throw new IOException( "Connection is closed" );
        }
        con.sendMessage( req.toByteArray() );
    }


    private void sendMessageBeforeDeadline( Request request, CallbackQueue<?> callback ) throws IOException, TimeoutException {
        long timeoutMillis = callback.getTimeoutMillis();
        if ( timeoutMillis <= 0 ) {
            sendMessage( request );
            return;
        }
        long deadline = callback.getDeadlineNanos();
        if ( deadline - System.nanoTime() <= 0 ) {
            throw new RequestDeadline.BeforeSendTimeout( timeoutMillis );
        }
        byte[] payload = request.toByteArray();
        long remaining = deadline - System.nanoTime();
        if ( remaining <= 0 ) {
            throw new RequestDeadline.BeforeSendTimeout( timeoutMillis );
        }

        AtomicReference<WriteState> state = new AtomicReference<>( WriteState.READY );
        ScheduledFuture<?> watchdog = WRITE_DEADLINES.schedule( () -> {
            // Before the writer starts, an expired request cannot have reached the server.
            if ( !state.compareAndSet( WriteState.READY, WriteState.FINISHED )
                    && state.get() == WriteState.WRITING
                    && !callback.hasTerminalResponse()
                    && state.compareAndSet( WriteState.WRITING, WriteState.EXPIRED ) ) {
                abort();
            }
        }, remaining, TimeUnit.NANOSECONDS );
        IOException failure = null;
        try {
            if ( closed ) {
                throw new IOException( "Connection is closed" );
            }
            boolean sent = con.trySendMessage( payload, deadline,
                    () -> deadline - System.nanoTime() > 0 && state.compareAndSet( WriteState.READY, WriteState.WRITING ) );
            if ( !sent ) {
                throw new RequestDeadline.BeforeSendTimeout( timeoutMillis );
            }
        } catch ( IOException e ) {
            failure = e;
        } finally {
            state.compareAndSet( WriteState.WRITING, WriteState.FINISHED );
            watchdog.cancel( false );
        }

        if ( state.get() == WriteState.EXPIRED || (failure == null && deadline - System.nanoTime() <= 0 && !callback.hasTerminalResponse()) ) {
            TimeoutException timeout = writeTimeout( timeoutMillis );
            if ( failure != null ) {
                timeout.addSuppressed( failure );
            }
            throw timeout;
        }
        if ( failure != null ) {
            throw failure;
        }
    }


    private static TimeoutException writeTimeout( long timeoutMillis ) {
        return new TimeoutException( "Prism request send did not complete within " + timeoutMillis + " ms." );
    }


    private enum WriteState {
        READY, WRITING, FINISHED, EXPIRED
    }


    private Response receiveMessage() throws IOException {
        return Response.parseFrom( con.receiveMessage() );
    }


    private void readResponses() {
        try {
            while ( true ) {
                Response resp = receiveMessage();
                if ( resp.getId() == 0 ) {
                    throw new ProtocolException( "Invalid message id" );
                }
                long id = resp.getId();
                CompletableFuture<Response> c = callbacks.get( id );
                CallbackQueue<?> cq = callbackQueues.get( id );
                // Prism 2.5+ peers may report an implicit transaction end before the normal reply.
                if ( transactionBoundaryEvents && !resp.getLast() && (resp.hasCommitResponse() || resp.hasRollbackResponse())
                        && (c != null || cq != null) ) {
                    transactionGeneration.incrementAndGet();
                    continue;
                }
                // Stamp replies in wire order, before an application thread can be delayed consuming them.
                if ( resp.getLast() && pendingTransactions.remove( id, resp.getTypeCase() ) ) {
                    transactionGeneration.incrementAndGet();
                }
                long generation = transactionGeneration.get();
                if ( c == null ) {
                    if ( cq != null ) {
                        if ( resp.hasErrorResponse() ) {
                            callbackQueues.remove( resp.getId() );
                            cq.onError( decodeErrorResponse( resp ), resp.getLast(), generation );
                        } else {
                            cq.onNext( resp, generation );
                            if ( resp.getLast() ) {
                                callbackQueues.remove( resp.getId() );
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
        } catch ( IOException e ) {
            PrismInterfaceServiceException failure = connectionFailure( e );
            callbacks.forEach( ( id, c ) -> c.completeExceptionally( failure ) );
            callbackQueues.forEach( ( id, cq ) -> cq.onError( failure ) );
        } catch ( Throwable t ) {
            abort();
            callbacks.forEach( ( id, c ) -> c.completeExceptionally( t ) );
            callbackQueues.forEach( ( id, cq ) -> cq.onError( t ) );
            log.error( "Unhandled exception", t );
            throw t;
        } finally {
            callbacks.clear();
            callbackQueues.clear();
        }
    }


    private Response waitForCompletion( CompletableFuture<Response> f, long timeoutNanos ) throws PrismInterfaceServiceException {
        try {
            if ( timeoutNanos == 0 ) {
                return f.get();
            } else {
                return f.get( timeoutNanos, TimeUnit.NANOSECONDS );
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new PrismInterfaceServiceException( e );
        } catch ( ExecutionException e ) {
            if ( e.getCause() instanceof PrismInterfaceServiceException ) {
                throw (PrismInterfaceServiceException) e.getCause();
            }
            throw new PrismInterfaceServiceException( e );
        } catch ( TimeoutException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    <E extends Exception> boolean withWriteReservation( int timeoutMillis, Transport.WriteOperation<E> operation ) throws E, InterruptedException {
        return con.withWriteReservation( timeoutMillis, operation );
    }


    private Response completeSynchronously( Request.Builder req, long timeout ) throws PrismInterfaceServiceException {
        if ( timeout > 0 ) {
            return completeBeforeDeadline( req, new RequestDeadline( timeout ) );
        }
        try {
            CompletableFuture<Response> f = new CompletableFuture<>();
            callbacks.put( req.getId(), f );
            sendMessage( req.build() );
            Response resp = waitForCompletion( f, TimeUnit.MILLISECONDS.toNanos( timeout ) );
            if ( resp.hasErrorResponse() ) {
                throw decodeErrorResponse( resp );
            }
            return resp;
        } catch ( IOException e ) {
            throw connectionFailure( e );
        } finally {
            callbacks.remove( req.getId() );
        }
    }


    private Response completeBeforeDeadline( Request.Builder req, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        CallbackQueue<Response> callback = deadline.callback( response -> response );
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessageBeforeDeadline( req.build(), callback );
            Response response = callback.takeNext();
            deadline.recordTransactionGeneration( callback );
            return response;
        } catch ( IOException e ) {
            throw connectionFailure( e );
        } catch ( TimeoutException e ) {
            throw new PrismInterfaceServiceException( e );
        } finally {
            callbackQueues.remove( req.getId() );
        }
    }


    private static PrismInterfaceServiceException decodeErrorResponse( Response response ) {
        ErrorResponse error = response.getErrorResponse();
        if ( !response.getLast() ) {
            return new PrismInterfaceServiceException( PrismInterfaceErrors.UNSPECIFIED, error.getMessage(),
                    new ProtocolException( "Prism error response must be final." ) );
        }
        ErrorDetails details = error.getDetails();
        String state = details.getState();
        return new PrismInterfaceServiceException.ServerError(
                error.getMessage(),
                SQLSTATE.matcher( state ).matches() ? state : PrismInterfaceErrors.UNSPECIFIED.state,
                details.hasErrorCode() ? details.getErrorCode() : PrismInterfaceErrors.UNSPECIFIED.errorCode );
    }


    ConnectionResponse connect( ConnectionRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionResponse();
    }


    ConnectionCheckResponse checkConnection( ConnectionCheckRequest msg, long timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionCheckRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionCheckResponse();
    }


    ConnectionPropertiesUpdateResponse updateConnectionProperties( ConnectionPropertiesUpdateRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionPropertiesUpdateRequest( msg );
        return completeSynchronously( req, timeout ).getConnectionPropertiesUpdateResponse();
    }


    ConnectionPropertiesUpdateResponse updateConnectionProperties( ConnectionPropertiesUpdateRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setConnectionPropertiesUpdateRequest( msg );
        return completeBeforeDeadline( req, deadline ).getConnectionPropertiesUpdateResponse();
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
            String causeMessage = e.getCause() == null ? null : e.getCause().getMessage();
            if ( causeMessage != null && causeMessage.contains( "An existing connection was forcibly closed by the remote host" ) ) {
                return DisconnectResponse.newBuilder().build();
            }
            throw e;
        }
    }


    long getTransactionGeneration() {
        return transactionGeneration.get();
    }


    void enableTransactionBoundaryEvents() {
        transactionBoundaryEvents = true;
    }


    CommitResponse commit( CommitRequest msg, int timeout ) throws PrismInterfaceServiceException {
        return commit( msg, new RequestDeadline( timeout ) );
    }


    CommitResponse commit( CommitRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        return completeTransaction( newMessage().setCommitRequest( msg ), deadline, Response.TypeCase.COMMIT_RESPONSE ).getCommitResponse();
    }


    RollbackResponse rollback( RollbackRequest msg, int timeout ) throws PrismInterfaceServiceException {
        return rollback( msg, new RequestDeadline( timeout ) );
    }


    RollbackResponse rollback( RollbackRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        return completeTransaction( newMessage().setRollbackRequest( msg ), deadline, Response.TypeCase.ROLLBACK_RESPONSE ).getRollbackResponse();
    }


    private Response completeTransaction( Request.Builder request, RequestDeadline deadline, Response.TypeCase responseType ) throws PrismInterfaceServiceException {
        pendingTransactions.put( request.getId(), responseType );
        try {
            return deadline.timeoutMillis == 0 ? completeSynchronously( request, 0 ) : completeBeforeDeadline( request, deadline );
        } finally {
            pendingTransactions.remove( request.getId() );
        }
    }


    void executeUnparameterizedStatement( ExecuteUnparameterizedStatementRequest msg, CallbackQueue<StatementResponse> callback ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementRequest( msg );
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessageBeforeDeadline( req.build(), callback );
        } catch ( IOException e ) {
            callbackQueues.remove( req.getId() );
            throw connectionFailure( e );
        } catch ( TimeoutException e ) {
            callbackQueues.remove( req.getId() );
            throw new PrismInterfaceServiceException( e );
        }
    }


    void discardCallback( CallbackQueue<?> callback ) {
        callbackQueues.values().remove( callback );
    }


    void executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest msg, CallbackQueue<StatementBatchResponse> callback ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementBatchRequest( msg );
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessage( req.build() );
        } catch ( IOException e ) {
            callbackQueues.remove( req.getId() );
            throw connectionFailure( e );
        }
    }


    StatementBatchResponse executeUnparameterizedStatementBatch( ExecuteUnparameterizedStatementBatchRequest msg, long timeout, IntConsumer acceptBatchId ) throws PrismInterfaceServiceException {
        return executeUnparameterizedStatementBatch( msg, new RequestDeadline( timeout ), acceptBatchId );
    }


    StatementBatchResponse executeUnparameterizedStatementBatch(
            ExecuteUnparameterizedStatementBatchRequest msg, RequestDeadline deadline, IntConsumer acceptBatchId ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteUnparameterizedStatementBatchRequest( msg );
        return completeBatch( req, msg.getStatementsCount(), deadline, acceptBatchId );
    }


    PreparedStatementSignature prepareIndexedStatement( PrepareStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setPrepareIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getPreparedStatementSignature();
    }


    public PreparedStatementSignature prepareNamedStatement( PrepareStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setPrepareNamedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getPreparedStatementSignature();
    }


    StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        return executeIndexedStatement( msg, (long) timeout );
    }


    StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, long timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getStatementResult();
    }


    StatementResult executeIndexedStatement( ExecuteIndexedStatementRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementRequest( msg );
        return completeBeforeDeadline( req, deadline ).getStatementResult();
    }


    public StatementResult executeNamedStatement( ExecuteNamedStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteNamedStatementRequest( msg );
        return completeSynchronously( req, timeout ).getStatementResult();
    }


    StatementBatchResponse executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest msg, long timeout ) throws PrismInterfaceServiceException {
        return executeIndexedStatementBatch( msg, new RequestDeadline( timeout ) );
    }


    StatementBatchResponse executeIndexedStatementBatch( ExecuteIndexedStatementBatchRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setExecuteIndexedStatementBatchRequest( msg );
        return completeBatch( req, msg.getParametersCount(), deadline, null );
    }


    private StatementBatchResponse completeBatch(
            Request.Builder req, int batchSize, RequestDeadline deadline, IntConsumer acceptBatchId ) throws PrismInterfaceServiceException {
        CallbackQueue<Response> callback = deadline.callback( response -> response );
        StatementBatchResponse completed = null;
        boolean firstFrame = true;
        boolean rawBatch = acceptBatchId != null;
        int batchId = rawBatch ? 0 : req.getExecuteIndexedStatementBatchRequest().getStatementId();
        try {
            callbackQueues.put( req.getId(), callback );
            sendMessageBeforeDeadline( req.build(), callback );
            while ( true ) {
                Response response = callback.takeNext();
                if ( !response.hasStatementBatchResponse() ) {
                    throw invalidBatchResponse( "Expected a Prism batch response, received " + response.getTypeCase() + "." );
                }
                StatementBatchResponse batch = response.getStatementBatchResponse();
                validateBatchPayload( batch, batchSize, response.getLast(), !rawBatch );
                if ( firstFrame && rawBatch ) {
                    batchId = batch.getBatchId();
                    if ( batchId == 0 ) {
                        throw invalidBatchResponse( "Initial Prism batch response is missing its batch ID." );
                    }
                    acceptBatchId.accept( batchId );
                } else if ( batch.getBatchId() != batchId ) {
                    throw invalidBatchResponse( "Unexpected Prism batch ID " + batch.getBatchId() + ", expected " + batchId + "." );
                }
                if ( response.getLast() ) {
                    return batch;
                }
                boolean acknowledgment = firstFrame && rawBatch && batch.getScalarsCount() == 0;
                if ( !acknowledgment && (completed == null || batch.getScalarsCount() > 0) ) {
                    completed = batch;
                }
                firstFrame = false;
            }
        } catch ( IOException e ) {
            throw connectionFailure( e );
        } catch ( TimeoutException e ) {
            throw new PrismInterfaceServiceException( e );
        } catch ( PrismInterfaceServiceException.ServerError error ) {
            throw completed == null ? error : error.withCompletedBatch( completed );
        } finally {
            deadline.recordTransactionGeneration( callback );
            callbackQueues.remove( req.getId() );
        }
    }


    private static void validateBatchPayload( StatementBatchResponse batch, int batchSize, boolean last, boolean prepared ) throws PrismInterfaceServiceException {
        int count = batch.getScalarsCount();
        boolean aggregateCount = prepared && count == 1 && batch.getGeneratedKeysCount() == 0;
        if ( count > batchSize || (last && count != batchSize && !aggregateCount) ) {
            throw invalidBatchResponse( "Invalid Prism batch update count length " + count + " for " + batchSize + " entries." );
        }
        if ( batch.getGeneratedKeysCount() > 0 ) {
            boolean[] seen = new boolean[count];
            for ( BatchGeneratedKeys keys : batch.getGeneratedKeysList() ) {
                int index = keys.getStatementIndex();
                if ( index < 0 || index >= count || seen[index] ) {
                    throw invalidBatchResponse( "Invalid or duplicate Prism generated-key batch index " + index + "." );
                }
                seen[index] = true;
            }
        }
    }


    private static PrismInterfaceServiceException invalidBatchResponse( String message ) {
        return new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, message, new ProtocolException( message ) );
    }


    Frame fetchResult( FetchRequest msg, int timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setFetchRequest( msg );
        return completeSynchronously( req, timeout ).getFrame();
    }


    CloseStatementResponse closeStatement( CloseStatementRequest msg, int timeout ) throws PrismInterfaceServiceException {
        return closeStatement( msg, (long) timeout );
    }


    CloseStatementResponse closeStatement( CloseStatementRequest msg, long timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        return completeSynchronously( req, timeout ).getCloseStatementResponse();
    }


    CloseStatementResponse closeStatement( CloseStatementRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseStatementRequest( msg );
        return completeBeforeDeadline( req, deadline ).getCloseStatementResponse();
    }


    CloseResultResponse closeResult( CloseResultRequest msg, int timeout ) throws PrismInterfaceServiceException {
        return closeResult( msg, (long) timeout );
    }


    CloseResultResponse closeResult( CloseResultRequest msg, long timeout ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseResultRequest( msg );
        return completeSynchronously( req, timeout ).getCloseResultResponse();
    }


    CloseResultResponse closeResult( CloseResultRequest msg, RequestDeadline deadline ) throws PrismInterfaceServiceException {
        Request.Builder req = newMessage();
        req.setCloseResultRequest( msg );
        return completeBeforeDeadline( req, deadline ).getCloseResultResponse();
    }

}

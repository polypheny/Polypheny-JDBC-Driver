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

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

@Slf4j
public class PolyphenyStatement implements Statement {

    @Getter
    private PolyConnection polyConnection;
    protected volatile ResultSet currentResult;
    protected long currentUpdateCount;
    @Getter
    protected int statementId;

    private final Object resultsLock = new Object();
    private volatile boolean isClosed;
    private volatile boolean closedAtTransactionEnd;
    private volatile boolean clearingResults;
    private ExecutionDeadline cleanupDeadline;
    private RelationalFrame generatedKeysFrame;
    private PolyphenyResultSetProperties generatedKeysProperties;
    private PolyphenyResultSet generatedKeysResult;
    private long generatedKeysGeneration = -1;
    protected PolyphenyStatementProperties properties;

    // Value used to represent that no value is set for the update count according to JDBC.
    protected static final int NO_UPDATE_COUNT = -1;
    protected static final int NO_STATEMENT_ID = -1;

    protected List<String> statementBatch;


    public PolyphenyStatement( PolyConnection connection, PolyphenyStatementProperties properties ) throws SQLException {
        this.polyConnection = connection;
        this.properties = properties;
        this.isClosed = false;
        this.statementBatch = new LinkedList<>();
        this.properties.setPolyphenyStatement( this );
        this.statementId = NO_STATEMENT_ID;
        this.currentResult = null;
        this.currentUpdateCount = NO_UPDATE_COUNT;
    }


    public boolean hasStatementId() {
        return statementId != NO_STATEMENT_ID;
    }


    protected PrismInterfaceClient getClient() {
        return polyConnection.getPrismInterfaceClient();
    }


    protected int longToInt( long longNumber ) {
        return Math.toIntExact( longNumber );
    }


    private void prepareForReExecution() throws SQLException {
        clearExecutionResults();
        releaseStatementHandle();
    }


    private void releaseStatementHandle() throws SQLException {
        if ( statementId != NO_STATEMENT_ID ) {
            if ( !polyConnection.isClosed() ) {
                closeResource( timeout -> getClient().closeStatement( statementId, timeout ) );
            }
            statementId = NO_STATEMENT_ID;
        }
    }


    protected final void clearExecutionResults() throws SQLException {
        clearExecutionResults( false );
    }


    private void clearExecutionResults( boolean closing ) throws SQLException {
        clearingResults = true;
        try {
            ResultSet previousResult;
            synchronized ( resultsLock ) {
                if ( !closing && (isLocallyClosed() || closedOnCompletionByTransaction()) ) {
                    throw closedStatement();
                }
                clearGeneratedKeys();
                previousResult = currentResult;
            }
            if ( previousResult != null ) {
                previousResult.close();
            }
            currentUpdateCount = NO_UPDATE_COUNT;
        } finally {
            clearingResults = false;
        }
    }


    private void clearGeneratedKeys() {
        if ( generatedKeysResult != null ) {
            generatedKeysResult.markClosed();
        }
        generatedKeysResult = null;
        generatedKeysFrame = null;
        generatedKeysProperties = null;
        generatedKeysGeneration = -1;
    }


    final void closeResultsFromEarlierTransactions( long generation ) throws SQLException {
        boolean closeStatement;
        synchronized ( resultsLock ) {
            if ( isLocallyClosed() ) {
                return;
            }
            boolean closedDependentResult = false;
            if ( currentResultClosedByTransaction( generation ) ) {
                // Prism has already closed the server cursor.
                clearCurrentResult();
                closedDependentResult = true;
            }
            if ( generatedKeysClosedByTransaction( generation ) ) {
                closedDependentResult |= generatedKeysFrame != null;
                clearGeneratedKeys();
            }
            closeStatement = closedDependentResult && canCloseAfterDependentResults();
            if ( closeStatement ) {
                closedAtTransactionEnd = true;
            }
        }
        if ( closeStatement ) {
            closeAfterTransaction();
        }
    }


    private boolean currentResultClosedByTransaction( long generation ) {
        return currentResult instanceof PolyphenyResultSet && ((PolyphenyResultSet) currentResult).closedByTransaction( generation );
    }


    private boolean generatedKeysClosedByTransaction( long generation ) {
        return generatedKeysProperties != null && closedByTransaction( generatedKeysProperties.getResultSetHoldability(), generatedKeysGeneration, generation );
    }


    private boolean canCloseAfterDependentResults() throws SQLException {
        return currentResult == null && !hasOpenGeneratedKeys() && properties.isCloseOnCompletion();
    }


    private void closeAfterTransaction() {
        try {
            close();
        } catch ( SQLException e ) {
            log.warn( "Could not close a completed statement after the transaction finished", e );
        }
    }


    private void clearCurrentResult() {
        if ( currentResult instanceof PolyphenyResultSet ) {
            ((PolyphenyResultSet) currentResult).markClosed();
        }
        currentResult = null;
    }


    public void notifyResultClosure() throws SQLException {
        if ( !polyConnection.isClosed() ) {
            closeResource( timeout -> getClient().closeResult( statementId, timeout ) );
        }
        boolean canClose;
        synchronized ( resultsLock ) {
            clearCurrentResult();
            canClose = !clearingResults && !hasOpenGeneratedKeys();
        }
        if ( !polyConnection.isClosed() && canClose && isCloseOnCompletion() ) {
            close();
        }
    }


    private boolean hasOpenGeneratedKeys() throws SQLException {
        return generatedKeysFrame != null && (generatedKeysResult == null || !generatedKeysResult.isClosed());
    }


    private void closeResource( CloseRequest request ) throws SQLException {
        ExecutionDeadline deadline = cleanupDeadline;
        try {
            RequestDeadline requestDeadline = deadline == null ? new RequestDeadline( polyConnection.getNetworkTimeout() ) : deadline.requestDeadline;
            request.execute( requestDeadline );
        } catch ( PrismInterfaceServiceException failure ) {
            throw polyConnection.handleIncompleteRequest( failure, deadline == null || deadline.networkDeadline );
        }
    }


    @FunctionalInterface
    private interface CloseRequest {

        void execute( RequestDeadline deadline ) throws PrismInterfaceServiceException;

    }


    void notifyGeneratedKeysClosure( PolyphenyResultSet result ) throws SQLException {
        boolean canClose;
        synchronized ( resultsLock ) {
            canClose = !clearingResults && result == generatedKeysResult && generatedKeysFrame != null && currentResult == null;
        }
        if ( canClose && !polyConnection.isClosed() && isCloseOnCompletion() ) {
            close();
        }
    }


    protected void captureGeneratedKeys( StatementResult result, long transactionGeneration ) throws SQLException {
        if ( result.hasGeneratedKeys() ) {
            storeGeneratedKeys( GeneratedKeys.requireComplete( result.getGeneratedKeys() ), transactionGeneration, !result.hasFrame() );
        }
    }


    protected void captureGeneratedKeys( StatementBatchResponse result, long transactionGeneration ) throws SQLException {
        if ( result.getGeneratedKeysCount() > 0 ) {
            storeGeneratedKeys( GeneratedKeys.combine( result.getGeneratedKeysList() ).getRelationalFrame(), transactionGeneration, true );
        }
    }


    private void storeGeneratedKeys( RelationalFrame keys, long transactionGeneration, boolean keysOnly ) throws SQLException {
        PolyphenyResultSetProperties keyProperties = properties.toResultSetProperties();
        long maxRows = keyProperties.getLargeMaxRows();
        if ( maxRows > 0 && maxRows < keys.getRowsCount() ) {
            keys = keys.toBuilder().clearRows().addAllRows( keys.getRowsList().subList( 0, (int) maxRows ) ).build();
        }
        boolean closeStatement = false;
        synchronized ( resultsLock ) {
            if ( !isLocallyClosed() ) {
                if ( closedByTransaction( keyProperties.getResultSetHoldability(), transactionGeneration, getClient().getTransactionGeneration() ) ) {
                    closeStatement = keysOnly && canCloseAfterDependentResults();
                    if ( closeStatement ) {
                        closedAtTransactionEnd = true;
                    }
                } else {
                    generatedKeysFrame = keys;
                    generatedKeysProperties = keyProperties;
                    generatedKeysGeneration = transactionGeneration;
                }
            }
        }
        if ( closeStatement ) {
            closeAfterTransaction();
        }
    }


    final BatchUpdateException batchFailure( PrismInterfaceServiceException.ServerError failure, long transactionGeneration ) {
        StatementBatchResponse completed = failure.completedBatch();
        long[] counts = completed == null ? null : completed.getScalarsList().stream().mapToLong( Long::longValue ).toArray();
        BatchUpdateException batch = new BatchUpdateException( failure.getMessage(), failure.getSQLState(), failure.getErrorCode(), counts, failure );
        if ( completed != null ) {
            try {
                captureGeneratedKeys( completed, transactionGeneration );
            } catch ( SQLException malformedKeys ) {
                batch.addSuppressed( malformedKeys );
            }
        }
        return batch;
    }


    @Override
    public void close() throws SQLException {
        synchronized ( resultsLock ) {
            if ( isClosed ) {
                return;
            }
            isClosed = true;
        }
        boolean retryable = false;
        try {
            SQLException resultFailure = null;
            try {
                clearExecutionResults( true );
            } catch ( SQLException failure ) {
                resultFailure = failure;
            }
            try {
                releaseStatementHandle();
            } catch ( SQLException failure ) {
                retryable = failure.getCause() instanceof RequestDeadline.BeforeSendTimeout
                        || failure instanceof PrismInterfaceServiceException.InterruptedBeforeSend;
                if ( resultFailure == null ) {
                    throw failure;
                }
                resultFailure.addSuppressed( failure );
            }
            if ( resultFailure != null ) {
                throw resultFailure;
            }
        } finally {
            if ( retryable && !polyConnection.isClosed() ) {
                synchronized ( resultsLock ) {
                    if ( statementId != NO_STATEMENT_ID ) {
                        isClosed = false;
                    }
                }
            } else {
                dispose();
            }
        }
    }


    final void dispose() {
        synchronized ( resultsLock ) {
            isClosed = true;
            clearGeneratedKeys();
            clearCurrentResult();
            currentUpdateCount = NO_UPDATE_COUNT;
            statementId = NO_STATEMENT_ID;
        }
        polyConnection.endTracking( this );
    }


    protected int getTimeout() throws SQLException {
        return Math.min( getConnection().getNetworkTimeout(), properties.getQueryTimeoutSeconds() * 1000 );
    }


    protected void throwIfClosed() throws SQLException {
        if ( isClosed() ) {
            throw closedStatement();
        }
    }


    private static PrismInterfaceServiceException closedStatement() {
        return new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Illegal operation for a closed statement" );
    }


    static boolean closedByTransaction( int holdability, long resultGeneration, long currentGeneration ) {
        return holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT && resultGeneration >= 0 && resultGeneration < currentGeneration;
    }


    protected final ResultSet publishResult( Frame frame, long transactionGeneration ) throws SQLException {
        throwIfNotRelational( frame );
        PolyphenyResultSet result = new PolyphenyResultSet( this, frame, properties.toResultSetProperties() );
        result.setTransactionGeneration( transactionGeneration );
        boolean closeStatement = false;
        synchronized ( resultsLock ) {
            if ( isLocallyClosed() ) {
                result.markClosed();
                throw closedStatement();
            }
            if ( result.closedByTransaction( getClient().getTransactionGeneration() ) ) {
                result.markClosed();
                closeStatement = canCloseAfterDependentResults();
                if ( closeStatement ) {
                    closedAtTransactionEnd = true;
                }
            } else {
                currentResult = result;
            }
        }
        checkPublishedResult( result, closeStatement );
        if ( closeStatement ) {
            closeAfterTransaction();
        }
        return result;
    }


    private ResultSet checkPublishedResult( PolyphenyResultSet result, boolean closingAfterTransaction ) throws SQLException {
        // The connection can be marked closed before its abort has disposed of every statement.
        if ( isClosed || (closedAtTransactionEnd && !closingAfterTransaction) || polyConnection.isClosed() ) {
            result.markClosed();
            throw closedStatement();
        }
        return result;
    }


    protected void throwIfNotRelational( Frame frame ) throws SQLException {
        if ( frame.getResultCase() == ResultCase.RELATIONAL_FRAME ) {
            return;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a relational result" );
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        ExecutionResult<StatementResult> execution = executeUnparameterized( statement, null );
        StatementResult result = execution.value;
        if ( !result.hasFrame() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a single ResultSet" );
        }
        return publishResult( result.getFrame(), execution.transactionGeneration );
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        return longToInt( executeLargeUpdate( statement ) );
    }


    @Override
    public long executeLargeUpdate( String statement ) throws SQLException {
        return executeUpdateWithKeys( statement, null );
    }


    private long executeUpdateWithKeys( String sql, GeneratedKeysRequest keys ) throws SQLException {
        ExecutionResult<StatementResult> execution = executeUnparameterized( sql, keys );
        StatementResult result = execution.value;
        if ( result.hasFrame() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must not produce a ResultSet" );
        }
        captureGeneratedKeys( result, execution.transactionGeneration );
        currentUpdateCount = result.getScalar();
        return currentUpdateCount;
    }


    private ExecutionResult<StatementResult> executeUnparameterized( String statement, GeneratedKeysRequest keys ) throws SQLException {
        throwIfClosed();
        PrismInterfaceClient client = getClient();
        if ( keys != null ) {
            GeneratedKeys.requireSupported( client );
        }
        clearBatch();
        return executeWithDeadline( this::prepareForReExecution, deadline -> {
            CallbackQueue<StatementResponse> callback = deadline.callback( Response::getStatementResponse );
            String namespaceName = getConnection().getSchema();
            String language = PropertyUtils.getSQL_LANGUAGE_NAME();
            int clientTimeout = (int) Math.min( deadline.remainingMillis(), Integer.MAX_VALUE );
            try {
                if ( keys == null ) {
                    client.executeUnparameterizedStatement( namespaceName, language, statement, callback, clientTimeout );
                } else {
                    client.executeUnparameterizedStatement( namespaceName, language, statement, keys, callback, clientTimeout );
                }
                while ( true ) {
                    StatementResponse response = callback.takeNext();
                    if ( !hasStatementId() ) {
                        statementId = response.getStatementId();
                    }
                    if ( response.hasResult() ) {
                        awaitCompletion( callback );
                        deadline.recordTransactionGeneration( callback );
                        return response.getResult();
                    }
                }
            } finally {
                client.discardCallback( callback );
            }
        } );
    }


    private void awaitCompletion( CallbackQueue<?> callback ) throws SQLException {
        try {
            callback.awaitSuccessfulCompletion();
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
        }
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        throwIfClosed();
        return properties.getMaxFieldSize();
    }


    @Override
    public void setMaxFieldSize( int max ) throws SQLException {
        throwIfClosed();
        properties.setMaxFieldSize( max );
    }


    @Override
    public long getLargeMaxRows() throws SQLException {
        throwIfClosed();
        return properties.getLargeMaxRows();
    }


    @Override
    public int getMaxRows() throws SQLException {
        throwIfClosed();
        return longToInt( getLargeMaxRows() );
    }


    @Override
    public void setLargeMaxRows( long max ) throws SQLException {
        throwIfClosed();
        properties.setLargeMaxRows( max );
    }


    @Override
    public void setMaxRows( int max ) throws SQLException {
        setLargeMaxRows( max );
    }


    @Override
    public void setEscapeProcessing( boolean enable ) throws SQLException {
        throwIfClosed();
        properties.setDoesEscapeProcessing( enable );
    }


    @Override
    public int getQueryTimeout() throws SQLException {
        throwIfClosed();
        return properties.getQueryTimeoutSeconds();
    }


    @Override
    public void setQueryTimeout( int seconds ) throws SQLException {
        throwIfClosed();
        if ( seconds < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal argument for max" );
        }
        properties.setQueryTimeoutSeconds( seconds );
    }


    @Override
    public void cancel() throws SQLException {
        throwIfClosed();
        // TODO TH: implement cancelling
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }


    @Override
    public void clearWarnings() throws SQLException {
        throwIfClosed();
    }


    @Override
    public void setCursorName( String s ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean execute( String statement ) throws SQLException {
        return executeWithKeys( statement, null );
    }


    private boolean executeWithKeys( String statement, GeneratedKeysRequest keys ) throws SQLException {
        ExecutionResult<StatementResult> execution = executeUnparameterized( statement, keys );
        StatementResult result = execution.value;
        captureGeneratedKeys( result, execution.transactionGeneration );
        if ( !result.hasFrame() ) {
            currentUpdateCount = result.getScalar();
            return false;
        }
        publishResult( result.getFrame(), execution.transactionGeneration );
        return true;
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        polyConnection.refreshTransactionResults();
        throwIfClosed();
        return currentResult;
    }


    @Override
    public long getLargeUpdateCount() throws SQLException {
        throwIfClosed();
        return currentUpdateCount;
    }


    @Override
    public int getUpdateCount() throws SQLException {
        return longToInt( getLargeUpdateCount() );
    }


    @Override
    public boolean getMoreResults() throws SQLException {
        throwIfClosed();
        ResultSet previousResult = currentResult;
        if ( previousResult != null ) {
            previousResult.close();
        }
        currentUpdateCount = NO_UPDATE_COUNT;
        return false;
    }


    @Override
    public void setFetchDirection( int direction ) throws SQLException {
        throwIfClosed();
        properties.setFetchDirection( direction );
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return properties.getFetchDirection();
    }


    @Override
    public void setFetchSize( int rows ) throws SQLException {
        throwIfClosed();
        properties.setFetchSize( rows );
    }


    @Override
    public int getFetchSize() throws SQLException {
        throwIfClosed();
        return properties.getFetchSize();
    }


    @Override
    public int getResultSetConcurrency() throws SQLException {
        throwIfClosed();
        return properties.getResultSetConcurrency();
    }


    @Override
    public int getResultSetType() throws SQLException {
        throwIfClosed();
        return properties.getResultSetType();
    }


    @Override
    public void addBatch( String sql ) throws SQLException {
        throwIfClosed();
        statementBatch.add( sql );
    }


    @Override
    public void clearBatch() throws SQLException {
        if ( statementBatch.isEmpty() ) {
            return;
        }
        statementBatch.clear();
    }


    @Override
    public long[] executeLargeBatch() throws SQLException {
        List<Long> scalars = executeUnparameterizedBatch();
        long[] updateCounts = new long[scalars.size()];
        for ( int i = 0; i < scalars.size(); i++ ) {
            updateCounts[i] = scalars.get( i );
        }
        return updateCounts;
    }


    @Override
    public int[] executeBatch() throws SQLException {
        List<Long> scalars = executeUnparameterizedBatch();
        int[] updateCounts = new int[scalars.size()];
        for ( int i = 0; i < scalars.size(); i++ ) {
            updateCounts[i] = longToInt( scalars.get( i ) );
        }
        return updateCounts;
    }


    private List<Long> executeUnparameterizedBatch() throws SQLException {
        try {
            throwIfClosed();
            ExecutionResult<StatementBatchResponse> execution = executeBatchRequest( this::prepareForReExecution, deadline -> {
                if ( statementBatch.isEmpty() ) {
                    return StatementBatchResponse.getDefaultInstance();
                }
                return getClient().executeUnparameterizedStatementBatch( buildBatchRequest(), deadline, id -> statementId = id );
            } );
            captureGeneratedKeys( execution.value, execution.transactionGeneration );
            return execution.value.getScalarsList();
        } finally {
            clearBatch();
        }
    }


    final ExecutionResult<StatementBatchResponse> executeBatchRequest( StatementCleanup cleanup, TimedRequest<StatementBatchResponse> request ) throws SQLException {
        return executeWithDeadline( cleanup, deadline -> {
            try {
                return request.execute( deadline );
            } catch ( PrismInterfaceServiceException.ServerError failure ) {
                throw batchFailure( failure, deadline.getTransactionGeneration() );
            }
        } );
    }


    final <T> ExecutionResult<T> executeWithDeadline( StatementCleanup cleanup, TimedRequest<T> request ) throws SQLException {
        ExecutionDeadline deadline = new ExecutionDeadline( polyConnection.getNetworkTimeout(), properties.getQueryTimeoutSeconds() );
        cleanupDeadline = deadline;
        try {
            cleanup.execute();
        } finally {
            cleanupDeadline = null;
        }
        try {
            throwIfClosed();
            deadline.requestDeadline.remainingMillis();
            T value = request.execute( deadline.requestDeadline );
            return new ExecutionResult<>( value, deadline.requestDeadline.getTransactionGeneration() );
        } catch ( PrismInterfaceServiceException failure ) {
            throw polyConnection.handleIncompleteRequest( failure, deadline.networkDeadline );
        } finally {
            polyConnection.refreshTransactionResults();
        }
    }


    @FunctionalInterface
    interface StatementCleanup {

        void execute() throws SQLException;

    }


    @FunctionalInterface
    interface TimedRequest<T> {

        T execute( RequestDeadline deadline ) throws SQLException;

    }


    static final class ExecutionResult<T> {

        final T value;
        final long transactionGeneration;


        private ExecutionResult( T value, long transactionGeneration ) {
            this.value = value;
            this.transactionGeneration = transactionGeneration;
        }

    }


    private static final class ExecutionDeadline {

        private final boolean networkDeadline;
        private final RequestDeadline requestDeadline;


        private ExecutionDeadline( int networkTimeout, int queryTimeoutSeconds ) {
            long queryTimeout = TimeUnit.SECONDS.toMillis( queryTimeoutSeconds );
            networkDeadline = networkTimeout > 0 && (queryTimeout == 0 || networkTimeout <= queryTimeout);
            requestDeadline = new RequestDeadline( networkDeadline ? networkTimeout : queryTimeout );
        }

    }


    List<ExecuteUnparameterizedStatementRequest> buildBatchRequest() throws SQLException {
        String namespaceName = getConnection().getSchema();
        boolean requestKeys = getClient().supportsGeneratedKeys();
        return statementBatch.stream()
                .map(
                        s -> {
                            ExecuteUnparameterizedStatementRequest.Builder builder = ExecuteUnparameterizedStatementRequest.newBuilder()
                                    .setStatement( s )
                                    .setFetchSize( properties.getFetchSize() )
                                    .setLanguageName( PropertyUtils.getSQL_LANGUAGE_NAME() );
                            if ( namespaceName != null ) {
                                builder.setNamespaceName( namespaceName );
                            }
                            if ( requestKeys ) {
                                builder.setGeneratedKeys( GeneratedKeys.ALL );
                            }
                            return builder.build();
                        }
                )
                .collect( Collectors.toList() );
    }


    @Override
    public Connection getConnection() throws SQLException {
        throwIfClosed();
        return polyConnection;
    }


    @Override
    public boolean getMoreResults( int i ) throws SQLException {
        if ( i == KEEP_CURRENT_RESULT || i == CLOSE_ALL_RESULTS ) {
            throw new SQLFeatureNotSupportedException();
        }
        if ( i != CLOSE_CURRENT_RESULT ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for closing behaviour: " + i );
        }
        return getMoreResults();
    }


    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        polyConnection.refreshTransactionResults();
        throwIfClosed();
        GeneratedKeys.requireSupported( getClient() );
        PolyphenyResultSet result;
        synchronized ( resultsLock ) {
            if ( isLocallyClosed() ) {
                throw closedStatement();
            }
            if ( generatedKeysResult == null ) {
                RelationalFrame keys = generatedKeysFrame == null ? RelationalFrame.getDefaultInstance() : generatedKeysFrame;
                if ( generatedKeysProperties == null ) {
                    generatedKeysProperties = properties.toResultSetProperties();
                    generatedKeysGeneration = getClient().getTransactionGeneration();
                }
                generatedKeysResult = new PolyphenyResultSet( this, keys, generatedKeysProperties );
                generatedKeysResult.setTransactionGeneration( generatedKeysGeneration );
            }
            result = generatedKeysResult;
        }
        return checkPublishedResult( result, false );
    }


    @Override
    public long executeLargeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        return executeUpdateWithKeys( sql, GeneratedKeys.requested( autogeneratedKeys ) );
    }


    @Override
    public int executeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, autogeneratedKeys ) );
    }


    @Override
    public long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        return executeUpdateWithKeys( sql, GeneratedKeys.requested( columnIndexes ) );
    }


    @Override
    public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, columnIndexes ) );
    }


    @Override
    public long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException {
        return executeUpdateWithKeys( sql, GeneratedKeys.requested( columnNames ) );
    }


    @Override
    public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, columnNames ) );
    }


    @Override
    public boolean execute( String sql, int autogeneratedKeys ) throws SQLException {
        return executeWithKeys( sql, GeneratedKeys.requested( autogeneratedKeys ) );
    }


    @Override
    public boolean execute( String sql, int[] columnIndexes ) throws SQLException {
        return executeWithKeys( sql, GeneratedKeys.requested( columnIndexes ) );
    }


    @Override
    public boolean execute( String sql, String[] columnNames ) throws SQLException {
        return executeWithKeys( sql, GeneratedKeys.requested( columnNames ) );
    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        throwIfClosed();
        return properties.getResultSetHoldability();
    }


    @Override
    public boolean isClosed() throws SQLException {
        if ( isLocallyClosed() || polyConnection.isClosed() ) {
            return true;
        }
        synchronized ( resultsLock ) {
            return isLocallyClosed() || closedOnCompletionByTransaction();
        }
    }


    private boolean closedOnCompletionByTransaction() throws SQLException {
        if ( !properties.isCloseOnCompletion() || (currentResult == null && generatedKeysFrame == null) ) {
            return false;
        }
        // A response can report closure before the requesting thread sweeps the connection's statements.
        long generation = getClient().getTransactionGeneration();
        boolean resultClosed = currentResultClosedByTransaction( generation );
        boolean keysClosed = generatedKeysClosedByTransaction( generation );
        return (resultClosed || (keysClosed && generatedKeysFrame != null))
                && (currentResult == null || resultClosed)
                && (keysClosed || !hasOpenGeneratedKeys());
    }


    private boolean isLocallyClosed() {
        return isClosed || closedAtTransactionEnd;
    }


    @Override
    public void setPoolable( boolean poolable ) throws SQLException {
        throwIfClosed();
        properties.setIsPoolable( poolable );
    }


    @Override
    public boolean isPoolable() throws SQLException {
        throwIfClosed();
        return properties.isPoolable();
    }


    @Override
    public void closeOnCompletion() throws SQLException {
        throwIfClosed();
        properties.setCloseOnCompletion( true );
    }


    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throwIfClosed();
        return properties.isCloseOnCompletion();
    }


    @Override
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) {
        return aClass.isInstance( this );
    }

}

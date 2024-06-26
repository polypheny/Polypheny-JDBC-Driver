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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;

public class PolyphenyStatement implements Statement {

    @Getter
    private PolyConnection polyConnection;
    protected ResultSet currentResult;
    protected long currentUpdateCount;
    @Getter
    protected int statementId;

    private boolean isClosed;
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
        if ( currentResult != null ) {
            currentResult.close();
        }
        currentUpdateCount = NO_UPDATE_COUNT;
        if ( statementId != NO_STATEMENT_ID ) {
            getClient().closeStatement( statementId, getTimeout() );
            statementId = NO_STATEMENT_ID;
        }
    }


    public void notifyResultClosure() throws SQLException {
        this.currentResult = null;
        getClient().closeResult( statementId, getTimeout() );
        if ( isCloseOnCompletion() ) {
            close();
        }
    }


    @Override
    public void close() throws SQLException {
        if ( isClosed ) {
            return;
        }
        polyConnection.endTracking( this );
        prepareForReExecution();
        isClosed = true;
    }


    protected int getTimeout() throws SQLException {
        return Math.min( getConnection().getNetworkTimeout(), properties.getQueryTimeoutSeconds() * 1000 );
    }


    protected void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Illegal operation for a closed statement" );
        }
    }


    protected void throwIfNotRelational( Frame frame ) throws SQLException {
        if ( frame.getResultCase() == ResultCase.RELATIONAL_FRAME ) {
            return;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a relational result" );
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        throwIfClosed();
        clearBatch();
        prepareForReExecution();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>( Response::getStatementResponse );
        String namespaceName = getConnection().getSchema();
        getClient().executeUnparameterizedStatement( namespaceName, PropertyUtils.getSQL_LANGUAGE_NAME(), statement, callback, getTimeout() );
        while ( true ) {
            StatementResponse response = callback.takeNext();
            if ( !hasStatementId() ) {
                statementId = response.getStatementId();
            }
            if ( !response.hasResult() ) {
                continue;
            }
            try {
                callback.awaitCompletion();
            } catch ( InterruptedException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
            }
            if ( !response.getResult().hasFrame() ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a single ResultSet" );
            }
            Frame frame = response.getResult().getFrame();
            throwIfNotRelational( frame );
            currentResult = new PolyphenyResultSet( this, frame, properties.toResultSetProperties() );
            return currentResult;
        }
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        throwIfClosed();
        clearBatch();
        prepareForReExecution();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>( Response::getStatementResponse );
        String namespaceName = getConnection().getSchema();
        getClient().executeUnparameterizedStatement( namespaceName, PropertyUtils.getSQL_LANGUAGE_NAME(), statement, callback, getTimeout() );
        while ( true ) {
            StatementResponse response = callback.takeNext();
            if ( !hasStatementId() ) {
                statementId = response.getStatementId();
            }
            if ( !response.hasResult() ) {
                continue;
            }
            try {
                callback.awaitCompletion();
            } catch ( InterruptedException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
            }
            if ( response.getResult().hasFrame() ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must not produce a ResultSet" );
            }
            currentUpdateCount = response.getResult().getScalar();
            return longToInt( currentUpdateCount );
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
        throwIfClosed();
        clearBatch();
        prepareForReExecution();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>( Response::getStatementResponse );
        String namespaceName = getConnection().getSchema();
        getClient().executeUnparameterizedStatement( namespaceName, PropertyUtils.getSQL_LANGUAGE_NAME(), statement, callback, getTimeout() );
        while ( true ) {
            StatementResponse response = callback.takeNext();
            if ( !hasStatementId() ) {
                statementId = response.getStatementId();
            }
            if ( !response.hasResult() ) {
                continue;
            }
            try {
                callback.awaitCompletion();
            } catch ( InterruptedException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
            }
            if ( !response.getResult().hasFrame() ) {
                currentUpdateCount = longToInt( response.getResult().getScalar() );
                return false;
            }
            Frame frame = response.getResult().getFrame();
            throwIfNotRelational( frame );
            currentResult = new PolyphenyResultSet( this, frame, properties.toResultSetProperties() );
            return true;
        }
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
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
        prepareForReExecution();
        // statements can not return multiple result sets
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
            prepareForReExecution();
            CallbackQueue<StatementBatchResponse> callback = new CallbackQueue<>( Response::getStatementBatchResponse );
            List<ExecuteUnparameterizedStatementRequest> requests = buildBatchRequest();
            clearBatch();
            getClient().executeUnparameterizedStatementBatch( requests, callback, getTimeout() );
            while ( true ) {
                StatementBatchResponse status = callback.takeNext();
                if ( !hasStatementId() ) {
                    statementId = status.getBatchId();
                }
                if ( status.getScalarsCount() == 0 ) {
                    continue;
                }
                try {
                    callback.awaitCompletion();
                } catch ( InterruptedException e ) {
                    throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
                }
                return status.getScalarsList();
            }
        } finally {
            clearBatch();
        }
    }


    List<ExecuteUnparameterizedStatementRequest> buildBatchRequest() throws SQLException {
        String namespaceName = getConnection().getSchema();
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
        throwIfClosed();
        prepareForReExecution();
        // statements can not return multiple result sets
        return false;
    }


    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public long executeLargeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, autogeneratedKeys ) );
    }


    @Override
    public long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, columnIndexes ) );
    }


    @Override
    public long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
        return longToInt( executeLargeUpdate( sql, columnNames ) );
    }


    @Override
    public boolean execute( String s, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean execute( String s, int[] ints ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean execute( String s, String[] strings ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        throwIfClosed();
        return properties.getResultSetHoldability();
    }


    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
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

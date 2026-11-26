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

import java.nio.channels.ClosedByInterruptException;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.types.PolyArray;
import org.polypheny.jdbc.types.PolyBlob;
import org.polypheny.jdbc.types.PolyClob;
import org.polypheny.jdbc.types.PolyStruct;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.PreparedStatementSignature;

@Slf4j
public class PolyConnection implements Connection {

    private PolyphenyConnectionProperties properties;

    private PolyphenyDatabaseMetadata databaseMetaData;
    private volatile boolean isClosed;

    private final Set<Statement> openStatements;
    private final AtomicLong appliedTransactionGeneration = new AtomicLong();
    private final ExecutorService completedStatementCloser = new ThreadPoolExecutor( 0, 1, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(), task -> {
        Thread thread = new Thread( task, "PrismCompletedStatementCloser" );
        thread.setDaemon( true );
        return thread;
    } );

    private Map<String, Class<?>> typeMap;


    private void throwIfClosed() throws SQLException {
        if ( isClosed() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.CONNECTION_LOST, "Illegal operation on closed connection." );
        }
    }


    private void throwIfAutoCommit() throws SQLException {
        if ( !isStrict() ) {
            return;
        }
        if ( properties.isAutoCommit() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Illegal operation on auto committing connection." );
        }
    }


    public PolyConnection( PolyphenyConnectionProperties connectionProperties, PolyphenyDatabaseMetadata databaseMetaData ) {
        this.properties = connectionProperties;
        databaseMetaData.setConnection( this );
        this.databaseMetaData = databaseMetaData;
        this.openStatements = new HashSet<>();
        this.typeMap = new HashMap<>();
        this.isClosed = false;
    }


    public boolean isStrict() {
        return properties.isStrict();
    }


    public void startTracking( Statement statement ) {
        synchronized ( openStatements ) {
            if ( !isClosed ) {
                openStatements.add( statement );
            }
        }
    }


    public void endTracking( Statement statement ) {
        synchronized ( openStatements ) {
            openStatements.remove( statement );
        }
    }


    public int getTimeout() {
        return properties.getNetworkTimeout();
    }


    public PrismInterfaceClient getPrismInterfaceClient() {
        return properties.getPrismInterfaceClient();
    }


    @Override
    public Statement createStatement() throws SQLException {
        throwIfClosed();
        PolyphenyStatement statement = new PolyphenyStatement( this, properties.toStatementProperties() );
        startTracking( statement );
        return statement;
    }


    public PolyStatement createPolyStatement() {
        return new PolyStatement( this );
    }


    @Override
    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        throwIfClosed();
        return prepareStatement( sql, properties.toStatementProperties(), null );
    }


    @Override
    public CallableStatement prepareCall( String sql ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public String nativeSQL( String sql ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void setAutoCommit( boolean autoCommit ) throws SQLException {
        throwIfClosed();
        if ( properties.isAutoCommit() == autoCommit ) {
            return;
        }
        withWriteReservation( deadline -> {
            if ( properties.isAutoCommit() == autoCommit ) {
                return;
            }
            try {
                // Prism changes only the mode flag. Finish the prior transaction in either direction.
                getPrismInterfaceClient().commitTransaction( deadline );
                properties.setAutoCommit( autoCommit, (int) deadline.remainingMillis() );
            } finally {
                refreshTransactionResults();
            }
        } );
    }


    private void withWriteReservation( DeadlineWriteOperation operation ) throws SQLException {
        throwIfClosed();
        RequestDeadline deadline = new RequestDeadline( getTimeout() );
        try {
            if ( !getPrismInterfaceClient().withWriteReservation( (int) deadline.remainingMillis(), () -> {
                throwIfClosed();
                operation.run( deadline );
            } ) ) {
                throw new PrismInterfaceServiceException( new RequestDeadline.BeforeSendTimeout( deadline.timeoutMillis ) );
            }
        } catch ( InterruptedException e ) {
            throw PrismInterfaceClient.interruptedBeforeWrite( e );
        } catch ( PrismInterfaceServiceException failure ) {
            throw handleIncompleteRequest( failure, true );
        }
    }


    @FunctionalInterface
    private interface DeadlineWriteOperation {

        void run( RequestDeadline deadline ) throws SQLException;

    }


    @Override
    public boolean getAutoCommit() throws SQLException {
        throwIfClosed();
        return properties.isAutoCommit();

    }


    @Override
    public void commit() throws SQLException {
        withWriteReservation( deadline -> {
            throwIfAutoCommit();
            try {
                getPrismInterfaceClient().commitTransaction( deadline );
            } finally {
                refreshTransactionResults();
            }
        } );
    }


    public void refreshTransactionResults() {
        refreshTransactionResults( false );
    }


    void refreshTransactionResults( boolean deferStatementClose ) {
        long generation = getPrismInterfaceClient().getTransactionGeneration();
        if ( generation <= appliedTransactionGeneration.get() ) {
            return;
        }
        for ( Statement statement : snapshotOpenStatements() ) {
            if ( statement instanceof PolyphenyStatement ) {
                try {
                    ((PolyphenyStatement) statement).closeResultsFromEarlierTransactions( generation, deferStatementClose );
                } catch ( SQLException e ) {
                    log.warn( "Could not close a completed statement after the transaction finished", e );
                }
            }
        }
        appliedTransactionGeneration.accumulateAndGet( generation, Math::max );
    }


    void closeCompletedStatementLater( PolyphenyStatement statement ) {
        synchronized ( openStatements ) {
            if ( !isClosed ) {
                completedStatementCloser.execute( statement::closeAfterTransaction );
            }
        }
    }


    @Override
    public void rollback() throws SQLException {
        withWriteReservation( deadline -> {
            throwIfAutoCommit();
            try {
                getPrismInterfaceClient().rollbackTransaction( deadline );
            } finally {
                refreshTransactionResults();
            }
        } );
    }


    @Override
    public void close() throws SQLException {
        if ( isClosed() ) {
            return;
        }
        try {
            for ( Statement openStatement : snapshotOpenStatements() ) {
                openStatement.close();
            }
            if ( !isClosed ) {
                getPrismInterfaceClient().unregister( properties.getNetworkTimeout() );
            }
        } finally {
            abortAfterIncompleteRequest();
        }
    }


    private Set<Statement> snapshotOpenStatements() {
        synchronized ( openStatements ) {
            return new HashSet<>( openStatements );
        }
    }


    @Override
    public boolean isClosed() {
        if ( !isClosed && getPrismInterfaceClient().isClosed() ) {
            abortAfterIncompleteRequest();
        }
        return isClosed;
    }


    boolean isTransportClosed() {
        // ResultSet may check this while its statement's result lock is held.
        return isClosed || getPrismInterfaceClient().isClosed();
    }


    SQLException handleIncompleteRequest( PrismInterfaceServiceException failure, boolean networkDeadline ) {
        if ( failure instanceof PrismInterfaceServiceException.ServerError || failure instanceof PrismInterfaceServiceException.InterruptedBeforeSend ) {
            return failure;
        }
        Throwable cause = failure.getCause();
        if ( cause instanceof RequestDeadline.BeforeSendTimeout ) {
            return new SQLTimeoutException( failure.getMessage(), cause );
        }
        if ( cause instanceof InterruptedException || cause instanceof ClosedByInterruptException
                || PrismInterfaceErrors.CONNECTION_FAILURE.state.equals( failure.getSQLState() ) ) {
            abortAfterIncompleteRequest();
            return failure;
        }
        if ( cause instanceof TimeoutException ) {
            // Closing the connection cannot undo a request the server has already received.
            abortAfterIncompleteRequest();
            if ( networkDeadline ) {
                return new PrismInterfaceServiceException( PrismInterfaceErrors.CONNECTION_LOST, failure.getMessage(), cause );
            }
            return new SQLTimeoutException( failure.getMessage(), cause );
        }
        return failure;
    }


    public <T> T executeNetworkRequest( NetworkRequest<T> request ) throws SQLException {
        int timeout = getNetworkTimeout();
        try {
            return request.execute( timeout );
        } catch ( PrismInterfaceServiceException failure ) {
            throw handleIncompleteRequest( failure, true );
        }
    }


    @FunctionalInterface
    public interface NetworkRequest<T> {

        T execute( int timeout ) throws PrismInterfaceServiceException;

    }


    Frame fetchPage( int statementId, int fetchSize ) throws PrismInterfaceServiceException {
        if ( isClosed() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.CONNECTION_LOST, "Illegal operation on closed connection." );
        }
        try {
            return getPrismInterfaceClient().fetchResult( statementId, fetchSize, getTimeout() );
        } catch ( PrismInterfaceServiceException failure ) {
            SQLException handled = handleIncompleteRequest( failure, true );
            throw handled instanceof PrismInterfaceServiceException ? (PrismInterfaceServiceException) handled : failure;
        }
    }


    /**
     * Closes the connection and releases locally tracked resources without further Prism requests.
     */
    public void abortAfterIncompleteRequest() {
        Set<Statement> statements;
        synchronized ( openStatements ) {
            if ( isClosed ) {
                return;
            }
            isClosed = true;
            completedStatementCloser.shutdownNow();
            statements = new HashSet<>( openStatements );
            openStatements.clear();
        }
        getPrismInterfaceClient().abort();
        for ( Statement openStatement : statements ) {
            if ( openStatement instanceof PolyphenyStatement ) {
                ((PolyphenyStatement) openStatement).dispose();
                continue;
            }
            try {
                openStatement.close();
            } catch ( SQLException e ) {
                log.warn( "Could not dispose of statement after aborting the connection", e );
            }
        }
    }


    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throwIfClosed();
        return databaseMetaData;
    }


    @Override
    public void setReadOnly( boolean readOnly ) throws SQLException {
        throwIfClosed();
        properties.setReadOnly( readOnly );
    }


    @Override
    public boolean isReadOnly() throws SQLException {
        throwIfClosed();
        return properties.isReadOnly();
    }


    @Override
    public void setCatalog( String catalog ) throws SQLException {
        throwIfClosed();
        // does nothing - just there for consistency
        properties.setCatalogName( catalog );
    }


    @Override
    public String getCatalog() {
        return properties.getCatalogName();

    }


    @Override
    public void setTransactionIsolation( int level ) throws SQLException {
        throwIfClosed();
        properties.setTransactionIsolation( level );
    }


    @Override
    public int getTransactionIsolation() throws SQLException {
        throwIfClosed();
        return properties.getTransactionIsolation();
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        throwIfClosed();
        return null;

    }


    @Override
    public void clearWarnings() throws SQLException {
        throwIfClosed();
    }


    @Override
    public Statement createStatement( int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency );
        PolyphenyStatement statement = new PolyphenyStatement( this, statementProperties );
        startTracking( statement );
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        return prepareStatement( sql, properties.toStatementProperties( resultSetType, resultSetConcurrency ), null );
    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return typeMap;
    }


    @Override
    public void setTypeMap( Map<String, Class<?>> map ) throws SQLException {
        this.typeMap = map;
    }


    @Override
    public void setHoldability( int holdability ) throws SQLException {
        throwIfClosed();
        properties.setResultSetHoldability( holdability );
    }


    @Override
    public int getHoldability() throws SQLException {
        throwIfClosed();
        return properties.getResultSetHoldability();
    }


    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Savepoint setSavepoint( String name ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void rollback( Savepoint savepoint ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void releaseSavepoint( Savepoint savepoint ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatement statement = new PolyphenyStatement( this, statementProperties );
        startTracking( statement );
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        return prepareStatement( sql, properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability ), null );
    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException {
        return prepareStatementWithKeys( sql, GeneratedKeys.requested( autoGeneratedKeys ) );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int[] columnIndexes ) throws SQLException {
        return prepareStatementWithKeys( sql, GeneratedKeys.requested( columnIndexes ) );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException {
        return prepareStatementWithKeys( sql, GeneratedKeys.requested( columnNames ) );
    }


    private PreparedStatement prepareStatementWithKeys( String sql, GeneratedKeysRequest keys ) throws SQLException {
        throwIfClosed();
        return prepareStatement( sql, properties.toStatementProperties(), keys );
    }


    private PreparedStatement prepareStatement( String sql, PolyphenyStatementProperties statementProperties, GeneratedKeysRequest keys ) throws SQLException {
        PrismInterfaceClient client = getPrismInterfaceClient();
        if ( keys != null ) {
            GeneratedKeys.requireSupported( client );
        }
        PreparedStatementSignature signature;
        try {
            String namespace = properties.getNamespaceName();
            String language = PropertyUtils.getSQL_LANGUAGE_NAME();
            signature = keys == null
                    ? client.prepareIndexedStatement( namespace, language, sql, getTimeout() )
                    : client.prepareIndexedStatement( namespace, language, sql, keys, getTimeout() );
        } catch ( PrismInterfaceServiceException failure ) {
            throw handleIncompleteRequest( failure, true );
        }
        PolyphenyPreparedStatement statement = new PolyphenyPreparedStatement( this, statementProperties, signature );
        startTracking( statement );
        return statement;
    }


    @Override
    public Clob createClob() throws SQLException {
        return new PolyClob();
    }


    @Override
    public Blob createBlob() throws SQLException {
        return new PolyBlob();
    }


    @Override
    public NClob createNClob() throws SQLException {
        // implements both clob and nclob as both are utf-8
        return new PolyClob();
    }


    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean isValid( int timeout ) throws SQLException {
        if ( timeout < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal argument for timeout" );
        }
        if ( isClosed() ) {
            return false;
        }
        long validationTimeout = timeout * 1000L;
        int networkTimeout = getTimeout();
        boolean networkDeadline = networkTimeout > 0 && (validationTimeout == 0 || networkTimeout <= validationTimeout);
        try {
            getPrismInterfaceClient().verifyConnection( networkDeadline ? networkTimeout : validationTimeout );
            return true;
        } catch ( PrismInterfaceServiceException failure ) {
            if ( networkDeadline || !(failure.getCause() instanceof TimeoutException) ) {
                handleIncompleteRequest( failure, networkDeadline );
            }
            return false;
        }
    }


    @Override
    public void setClientInfo( String name, String value ) throws SQLClientInfoException {
        validateClientInfoNames( Collections.singleton( name ) );
        try {
            executeNetworkRequest( timeout -> {
                getPrismInterfaceClient().setClientInfoProperty( name, value, timeout );
                return null;
            } );
        } catch ( SQLException e ) {
            ClientInfoStatus status = value != null && "22023".equals( e.getSQLState() )
                    ? ClientInfoStatus.REASON_VALUE_INVALID : ClientInfoStatus.REASON_UNKNOWN;
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), Collections.singletonMap( name, status ), e );
        }
    }


    @Override
    public void setClientInfo( Properties clientInfoProperties ) throws SQLClientInfoException {
        Set<String> pending = new HashSet<>( clientInfoProperties.stringPropertyNames() );
        validateClientInfoNames( pending );
        try {
            executeNetworkRequest( timeout -> {
                getPrismInterfaceClient().setClientInfoProperties( clientInfoProperties, timeout, pending );
                return null;
            } );
        } catch ( SQLException e ) {
            Map<String, ClientInfoStatus> failed = new HashMap<>();
            pending.forEach( name -> failed.put( name, ClientInfoStatus.REASON_UNKNOWN ) );
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), failed, e );
        }
    }


    private static void validateClientInfoNames( Set<String> names ) throws SQLClientInfoException {
        if ( names.stream().allMatch( PrismInterfaceClient::isValidClientInfoPropertyName ) ) {
            return;
        }
        Map<String, ClientInfoStatus> failed = new HashMap<>();
        for ( String name : names ) {
            failed.put( name, PrismInterfaceClient.isValidClientInfoPropertyName( name )
                    ? ClientInfoStatus.REASON_UNKNOWN : ClientInfoStatus.REASON_UNKNOWN_PROPERTY );
        }
        throw new SQLClientInfoException( "Invalid ClientInfo property name.", PrismInterfaceErrors.VALUE_ILLEGAL.state,
                PrismInterfaceErrors.VALUE_ILLEGAL.errorCode, failed );
    }


    @Override
    public String getClientInfo( String name ) throws SQLException {
        return getClientInfo().getProperty( name );
    }


    @Override
    public Properties getClientInfo() throws SQLClientInfoException {
        try {
            Properties properties = new Properties();
            properties.putAll( executeNetworkRequest( getPrismInterfaceClient()::getClientInfoProperties ) );
            return properties;
        } catch ( SQLException e ) {
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), null, e );
        }
    }


    @Override
    public Array createArrayOf( String typeName, Object[] elements ) throws SQLException {
        throwIfClosed();
        return new PolyArray( typeName, elements );
    }


    @Override
    public Struct createStruct( String typeName, Object[] attributes ) throws SQLException {
        throwIfClosed();
        return new PolyStruct( typeName, attributes );
    }


    @Override
    public void setSchema( String schema ) throws SQLException {
        throwIfClosed();
        try {
            properties.setNamespaceName( schema );
        } catch ( PrismInterfaceServiceException failure ) {
            throw handleIncompleteRequest( failure, true );
        }
    }


    @Override
    public String getSchema() throws SQLException {
        throwIfClosed();
        return properties.getNamespaceName();
    }


    @Override
    public void abort( Executor executor ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException {
        throwIfClosed();
        if ( executor == null ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Network timeout executor cannot be null." );
        }
        if ( milliseconds < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal argument for timeout" );
        }
        properties.setNetworkTimeout( milliseconds );
    }


    @Override
    public int getNetworkTimeout() throws SQLException {
        throwIfClosed();
        return properties.getNetworkTimeout();
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

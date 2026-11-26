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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.transport.Transport.WriteOperation;
import org.polypheny.jdbc.types.PolyArray;
import org.polypheny.jdbc.types.PolyBlob;
import org.polypheny.jdbc.types.PolyClob;
import org.polypheny.jdbc.types.PolyStruct;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.PreparedStatementSignature;

@Slf4j
public class PolyConnection implements Connection {

    private PolyphenyConnectionProperties properties;

    private PolyphenyDatabaseMetadata databaseMetaData;
    private volatile boolean isClosed;

    private final Set<Statement> openStatements;

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
        withWriteReservation( () -> {
            if ( properties.isAutoCommit() == autoCommit ) {
                return;
            }
            if ( autoCommit ) {
                // Prism changes only the mode flag. Keep writes from crossing the commit and the mode change.
                commit();
            }
            properties.setAutoCommit( autoCommit );
        } );
    }


    private void withWriteReservation( WriteOperation<SQLException> operation ) throws SQLException {
        throwIfClosed();
        int timeout = getTimeout();
        try {
            if ( !getPrismInterfaceClient().withWriteReservation( timeout, () -> {
                throwIfClosed();
                operation.run();
            } ) ) {
                throw new PrismInterfaceServiceException( new RequestDeadline.BeforeSendTimeout( timeout ) );
            }
        } catch ( InterruptedException e ) {
            throw PrismInterfaceClient.interruptedBeforeWrite( e );
        } catch ( PrismInterfaceServiceException failure ) {
            throw handleIncompleteRequest( failure, true );
        }
    }


    @Override
    public boolean getAutoCommit() throws SQLException {
        throwIfClosed();
        return properties.isAutoCommit();

    }


    @Override
    public void commit() throws SQLException {
        withWriteReservation( this::commitAndCloseResults );
    }


    private void commitAndCloseResults() throws SQLException {
        throwIfAutoCommit();
        getPrismInterfaceClient().commitTransaction( getNetworkTimeout() );
        for ( Statement statement : snapshotOpenStatements() ) {
            if ( statement instanceof PolyphenyStatement ) {
                try {
                    ((PolyphenyStatement) statement).closeResultsAtCommit();
                } catch ( SQLException e ) {
                    log.warn( "Could not close completed statement after committing the transaction", e );
                }
            }
        }
    }


    @Override
    public void rollback() throws SQLException {
        withWriteReservation( () -> {
            throwIfAutoCommit();
            getPrismInterfaceClient().rollbackTransaction( getNetworkTimeout() );
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


    SQLException handleIncompleteRequest( PrismInterfaceServiceException failure, boolean networkDeadline ) {
        if ( failure instanceof PrismInterfaceServiceException.ServerError ) {
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
        return getPrismInterfaceClient().checkConnection( timeout * 1000L );
    }


    @Override
    public void setClientInfo( String name, String value ) throws SQLClientInfoException {
        Properties clientInfoProperties = getClientInfo();
        clientInfoProperties.setProperty( name, value );
        try {
            getPrismInterfaceClient().setClientInfoProperties( clientInfoProperties, properties.getNetworkTimeout() );
        } catch ( PrismInterfaceServiceException e ) {
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), new HashMap<>(), e );
        }
    }


    @Override
    public void setClientInfo( Properties clientInfoProperties ) throws SQLClientInfoException {
        try {
            getPrismInterfaceClient().setClientInfoProperties( clientInfoProperties, properties.getNetworkTimeout() );
        } catch ( PrismInterfaceServiceException e ) {
            HashMap<String, ClientInfoStatus> failedOptions = new HashMap<>();
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), new HashMap<>(), e );
        }
    }


    @Override
    public String getClientInfo( String name ) throws SQLException {
        return getClientInfo().getProperty( name );
    }


    @Override
    public Properties getClientInfo() throws SQLClientInfoException {
        try {
            Properties properties = new Properties();
            properties.putAll( getPrismInterfaceClient().getClientInfoProperties( getNetworkTimeout() ) );
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
        properties.setNamespaceName( schema );
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

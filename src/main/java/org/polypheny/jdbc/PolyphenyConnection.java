package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
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
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executor;
import lombok.Getter;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.types.PolyphenyArray;
import org.polypheny.jdbc.types.PolyphenyBlob;
import org.polypheny.jdbc.types.PolyphenyClob;
import org.polypheny.jdbc.types.PolyphenyStruct;

public class PolyphenyConnection implements Connection {

    private PolyphenyConnectionProperties properties;

    private String url;

    private PolyphenyDatabaseMetadata databaseMetaData;
    private boolean isClosed;

    private boolean hasRunningTransaction;

    private HashSet<Statement> openStatements;

    private Map<String, Class<?>> typeMap;

    private void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new ProtoInterfaceServiceException( SQLErrors.CONNECTION_LOST, "Illegal operation on closed connection." );
        }
    }


    private void throwIfAutoCommit() throws SQLException {
        if ( properties.isAutoCommit() ) {
            throw new ProtoInterfaceServiceException( SQLErrors.OPERATION_ILLEGAL, "Illegal operation on auto committing connection." );
        }
    }


    private void throwIfRunningTransaction() throws SQLException {
        if ( hasRunningTransaction ) {
            throw new ProtoInterfaceServiceException( SQLErrors.OPERATION_ILLEGAL, "Illegal operation during running transaction." );
        }
    }

    public void removeStatement(Statement polyphenyStatement) {
        openStatements.remove( polyphenyStatement );
    }


    public PolyphenyConnection(
            PolyphenyConnectionProperties connectionProperties,
            PolyphenyDatabaseMetadata databaseMetaData ) {

        this.properties = connectionProperties;
        databaseMetaData.setConnection( this );
        this.databaseMetaData = databaseMetaData;
        this.openStatements = new HashSet<>();
        this.typeMap = new HashMap<>();
        this.isClosed = false;
    }


    public PolyphenyConnection(
            PolyphenyConnectionProperties connectionProperties,
            PolyphenyDatabaseMetadata databaseMetaData,
            long heartbeatInterval ) {

        this( connectionProperties, databaseMetaData );
        Timer heartbeatTimer = new Timer();
        heartbeatTimer.schedule( createNewHeartbeatTask(), 0, heartbeatInterval );
    }


    private TimerTask createNewHeartbeatTask() {
        Runnable runnable = () -> getProtoInterfaceClient().checkConnection( properties.getNetworkTimeout() );
        return new TimerTask() {
            @Override
            public void run() {
                runnable.run();
            }
        };
    }


    public void removeStatementFromOpen( Statement statement ) {
        if ( !openStatements.contains( statement ) ) {
            return;
        }
        openStatements.remove( statement );
    }


    public ProtoInterfaceClient getProtoInterfaceClient() {
        return properties.getProtoInterfaceClient();
    }


    @Override
    public Statement createStatement() throws SQLException {
        throwIfClosed();
        PolyphenyStatement statement = new PolyphenyStatement( this, properties.toStatementProperties() );
        openStatements.add( statement );
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql, getNetworkTimeout() );
        PolyphenyPreparedStatement statement = new PolyphenyPreparedStatement( this, properties.toStatementProperties(), signature );
        openStatements.add( statement );
        return statement;
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
        if ( hasRunningTransaction ) {
            commit();
        }
        properties.setAutoCommit( autoCommit );
    }


    @Override
    public boolean getAutoCommit() throws SQLException {
        throwIfClosed();
        return properties.isAutoCommit();

    }


    @Override
    public void commit() throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        getProtoInterfaceClient().commitTransaction( getNetworkTimeout() );
        hasRunningTransaction = false;
    }


    @Override
    public void rollback() throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        getProtoInterfaceClient().rollbackTransaction( getNetworkTimeout() );
    }


    @Override
    public void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        List<Statement> statements = new ArrayList<>( openStatements );
        for ( Statement statement : statements ) {
            statement.close();
            openStatements.remove( statement );
        }
        getProtoInterfaceClient().unregister( properties.getNetworkTimeout() );
        isClosed = true;
    }


    @Override
    public boolean isClosed() {
        return isClosed;
    }


    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throwIfClosed();
        return databaseMetaData;
    }


    @Override
    public void setReadOnly( boolean readOnly ) throws SQLException {
        throwIfClosed();
        throwIfRunningTransaction();
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
        return new PolyphenyStatement( this, statementProperties );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency );
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql, getNetworkTimeout() );
        return new PolyphenyPreparedStatement( this, statementProperties, signature );

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
        openStatements.add( statement );
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql, getNetworkTimeout() );
        PolyphenyPreparedStatement statement = new PolyphenyPreparedStatement( this, statementProperties, signature );
        openStatements.add( statement );
        return statement;
    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException {
        throwIfClosed();
        if ( !PropertyUtils.isValidAutogeneratedKeys( autoGeneratedKeys ) ) {
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Illegal argument for autogenerated keys" );
        }
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int[] columnIndexes ) throws SQLException {
        throwIfClosed();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException {
        throwIfClosed();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Clob createClob() throws SQLException {
        return new PolyphenyClob();

    }


    @Override
    public Blob createBlob() throws SQLException {
        return new PolyphenyBlob();

    }


    @Override
    public NClob createNClob() throws SQLException {
        // implements both clob and nclob as both are utf-8
        return new PolyphenyClob();
    }


    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean isValid( int timeout ) throws SQLException {
        if ( timeout < 0 ) {
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Illegal argument for timeout" );
        }
        // the proto-interface uses milliseconds for timeouts, jdbc uses seconds
        return getProtoInterfaceClient().checkConnection( timeout * 1000 );
    }


    @Override
    public void setClientInfo( String name, String value ) throws SQLClientInfoException {
        Properties clientInfoProperties = getClientInfo();
        clientInfoProperties.setProperty( name, value );
        try {
            getProtoInterfaceClient().setClientInfoProperties( clientInfoProperties, properties.getNetworkTimeout() );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new SQLClientInfoException( e.getMessage(), e.getSQLState(), e.getErrorCode(), new HashMap<String, ClientInfoStatus>(), e );
        }
    }


    @Override
    public void setClientInfo( Properties clientInfoProperties ) throws SQLClientInfoException {
        try {
            getProtoInterfaceClient().setClientInfoProperties( clientInfoProperties, properties.getNetworkTimeout() );
        } catch ( ProtoInterfaceServiceException e ) {
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
            properties.putAll( getProtoInterfaceClient().getClientInfoProperties( getNetworkTimeout() ) );
            return properties;
        } catch ( StatusRuntimeException | SQLException e ) {
            throw new SQLClientInfoException();
        }
    }


    @Override
    public Array createArrayOf( String typeName, Object[] elements ) throws SQLException {
        throwIfClosed();
        return new PolyphenyArray( typeName, elements );
    }


    @Override
    public Struct createStruct( String typeName, Object[] attributes ) throws SQLException {
        throwIfClosed();
        return new PolyphenyStruct( typeName, attributes );
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
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Illegal argument for timeout" );
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
        throw new ProtoInterfaceServiceException( SQLErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) {
        return aClass.isInstance( this );

    }

}

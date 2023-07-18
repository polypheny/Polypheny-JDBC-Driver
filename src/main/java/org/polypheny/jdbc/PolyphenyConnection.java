package org.polypheny.jdbc;

import java.sql.*;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

import io.grpc.StatusRuntimeException;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.proto.UserDefinedType;
import org.polypheny.jdbc.proto.UserDefinedTypesRequest;
import org.polypheny.jdbc.types.PolyphenyArray;
import org.polypheny.jdbc.types.PolyphenyBlob;
import org.polypheny.jdbc.types.PolyphenyClob;
import org.polypheny.jdbc.properties.PropertyUtils;

public class PolyphenyConnection implements Connection {

    private PolyphenyConnectionProperties properties;

    private String url;

    private PolyphenyDatabaseMetadata databaseMetaData;
    private boolean isClosed;

    private boolean hasRunningTransaction;

    private HashSet<Statement> openStatements;

    private void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "Illegal operation on closed connection." );
        }
    }


    private void throwIfAutoCommit() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "Illegal operation on auto committing connection." );
        }
    }


    private void throwIfRunningTransaction() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "Illegal operation during running transaction." );
        }
    }


    public PolyphenyConnection(PolyphenyConnectionProperties connectionProperties, PolyphenyDatabaseMetadata databaseMetaData ) {
        this.properties = connectionProperties;
        databaseMetaData.setConnection( this );
        this.databaseMetaData = databaseMetaData;
        openStatements = new HashSet<>();
    }

    public void removeStatementFromOpen(Statement statement) {
        if (!openStatements.contains(statement)) {
            return;
        }
        openStatements.remove(statement);
    }

    public ProtoInterfaceClient getProtoInterfaceClient() {
        return properties.getProtoInterfaceClient();
    }


    @Override
    public Statement createStatement() throws SQLException {
        throwIfClosed();
        PolyphenyStatement statement = new PolyphenyStatement( this, properties.toStatementProperties() );
        openStatements.add(statement);
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
        PolyphenyPreparedStatement statement = new PolyphenyPreparedStatement( this, properties.toStatementProperties(), signature );
        openStatements.add(statement);
        return statement;
    }


    @Override
    public CallableStatement prepareCall( String sql ) throws SQLException {
        throwIfClosed();
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
        try {
            getProtoInterfaceClient().commitTransaction();
            hasRunningTransaction = false;
        } catch ( ProtoInterfaceServiceException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public void rollback() throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        getProtoInterfaceClient().rollbackTransaction();
    }


    @Override
    public void close() throws SQLException {
        for (Statement statement : openStatements) {
            statement.close();
        }
        getProtoInterfaceClient().unregister();
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
        properties.setCatalogName(catalog);
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
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
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
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void setTypeMap( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException();
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
        throwIfClosed();
        throwIfAutoCommit();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Savepoint setSavepoint( String name ) throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void rollback( Savepoint savepoint ) throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public void releaseSavepoint( Savepoint savepoint ) throws SQLException {
        throwIfClosed();
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatement statement = new PolyphenyStatement( this, statementProperties );
        openStatements.add(statement);
        return statement;
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        PolyphenyStatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
        PolyphenyPreparedStatement statement = new PolyphenyPreparedStatement( this, statementProperties, signature );
        openStatements.add(statement);
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
            throw new SQLException( "Illegal argument for autogenerated keys" );
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
            throw new SQLException( "Illegal argument for timeout" );
        }
        return getProtoInterfaceClient().checkConnection( timeout );
    }


    @Override
    public void setClientInfo( String name, String value ) throws SQLClientInfoException {
        Properties properties = getClientInfo();
        properties.setProperty(name, value);
        getProtoInterfaceClient().setClientInfoProperties(properties);

    }


    @Override
    public void setClientInfo( Properties properties ) throws SQLClientInfoException {
        getProtoInterfaceClient().setClientInfoProperties(properties);
    }


    @Override
    public String getClientInfo( String name ) throws SQLException {
        return getClientInfo().getProperty(name);
    }


    @Override
    public Properties getClientInfo() throws SQLClientInfoException {
        try {
            Properties properties = new Properties();
            properties.putAll(getProtoInterfaceClient().getClientInfoProperties());
            return properties;
        } catch (StatusRuntimeException e) {
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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setSchema( String schema ) throws SQLException {
        throwIfClosed();
        properties.setNamespaceName(schema);
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
            throw new SQLException( "Illegal argument for timeout" );
        }
        if ( executor == null ) {
            throw new SQLException( "Executor must not be null" );
        }
        properties.setNetworkTimeout( milliseconds );
    }


    @Override
    public int getNetworkTimeout() throws SQLException {
        throwIfClosed();
        return properties.getNetworkTimeout();

    }


    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new SQLException("Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return aClass.isInstance(this);

    }
}

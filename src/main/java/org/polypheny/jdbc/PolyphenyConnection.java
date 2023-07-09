package org.polypheny.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import lombok.SneakyThrows;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.types.PolyphenyBlob;
import org.polypheny.jdbc.types.PolyphenyClob;
import org.polypheny.jdbc.utils.PropertyUtils;

public class PolyphenyConnection implements Connection {

    private ProtoInterfaceClient protoInterfaceClient;

    private ConnectionProperties properties;

    private String url;

    private PolyphenyDatabaseMetadata databaseMetaData;
    private boolean isClosed;

    private boolean hasRunningTransaction;


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


    public PolyphenyConnection( ProtoInterfaceClient protoInterfaceClient, PolyphenyDatabaseMetadata databaseMetaData ) {
        this.protoInterfaceClient = protoInterfaceClient;
        this.properties = new ConnectionProperties();
        databaseMetaData.setConnection( this );
        this.databaseMetaData = databaseMetaData;
    }


    public ProtoInterfaceClient getProtoInterfaceClient() {
        return protoInterfaceClient;
    }


    @Override
    public Statement createStatement() throws SQLException {
        throwIfClosed();
        return new PolyphenyStatement( this, properties.toStatementProperties() );
    }


    @Override
    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
        return new PolyphenyPreparedStatement( this, properties.toStatementProperties(), signature );
    }


    @Override
    public CallableStatement prepareCall( String sql ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public String nativeSQL( String sql ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
            protoInterfaceClient.commitTransaction();
        } catch ( ProtoInterfaceServiceException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public void rollback() throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void close() throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isClosed() throws SQLException {
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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getCatalog() throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setTransactionIsolation( int level ) throws SQLException {
        throwIfClosed();
        if ( !PropertyUtils.isValidIsolationLevel( level ) ) {
            throw new SQLException( "Illeagal argument for transaciton isolation level" );
        }
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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void clearWarnings() throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Statement createStatement( int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        StatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency );
        return new PolyphenyStatement( this, statementProperties );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );
        StatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency );
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
        return new PolyphenyPreparedStatement( this, statementProperties, signature );

    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency );

        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setTypeMap( Map<String, Class<?>> map ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void setHoldability( int holdability ) throws SQLException {
        throwIfClosed();
        if ( !PropertyUtils.isValidResultSetHoldability( holdability ) ) {
            throw new SQLException( "Illegal argument for result set holdability" );
        }
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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Savepoint setSavepoint( String name ) throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void rollback( Savepoint savepoint ) throws SQLException {
        throwIfClosed();
        throwIfAutoCommit();
        //TODO TH: throw error if safepoint is no longer valid
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void releaseSavepoint( Savepoint savepoint ) throws SQLException {
        throwIfClosed();
        //TODO TH: throw error if safepoint is no longer valid
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Statement createStatement( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        StatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        return new PolyphenyStatement( this, statementProperties );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        StatementProperties statementProperties = properties.toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
        PreparedStatementSignature signature = getProtoInterfaceClient().prepareIndexedStatement( sql );
        return new PolyphenyPreparedStatement( this, statementProperties, signature );
    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        PropertyUtils.throwIfInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );

        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public PreparedStatement prepareStatement( String sql, int autoGeneratedKeys ) throws SQLException {
        throwIfClosed();
        if ( !PropertyUtils.isValidAutogeneratedKeys( autoGeneratedKeys ) ) {
            throw new SQLException( "Illegal argument for autogenerated keys" );
        }
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public PreparedStatement prepareStatement( String sql, int[] columnIndexes ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public PreparedStatement prepareStatement( String sql, String[] columnNames ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean isValid( int timeout ) throws SQLException {
        if ( timeout < 0 ) {
            throw new SQLException( "Illegal argument for timeout" );
        }
        return getProtoInterfaceClient().checkConnection( timeout );
    }


    @SneakyThrows
    @Override
    public void setClientInfo( String name, String value ) throws SQLClientInfoException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @SneakyThrows
    @Override
    public void setClientInfo( Properties properties ) throws SQLClientInfoException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getClientInfo( String name ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Properties getClientInfo() throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Array createArrayOf( String typeName, Object[] elements ) throws SQLException {
        throwIfClosed();
        return null;

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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getSchema() throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void abort( Executor executor ) throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
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
    public <T> T unwrap( Class<T> iface ) throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean isWrapperFor( Class<?> iface ) throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }

}

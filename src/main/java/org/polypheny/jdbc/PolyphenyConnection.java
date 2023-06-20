package org.polypheny.jdbc;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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
import org.polypheny.jdbc.utils.ValidPropertyValues;

public class PolyphenyConnection implements Connection {

    private ProtoInterfaceClient protoInterfaceClient;

    private boolean isAutoCommit;
    private boolean isReadOnly;
    private int resultSetHoldability;
    private int networkTimeout;
    private int transactionIsolation;
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


    public PolyphenyConnection( ProtoInterfaceClient protoInterfaceClient ) {
        this.protoInterfaceClient = protoInterfaceClient;
    }


    public ProtoInterfaceClient getProtoInterfaceClient() {
        return protoInterfaceClient;
    }


    @Override
    public Statement createStatement() throws SQLException {
        throwIfClosed();
        return new PolyphenyStatement( this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, resultSetHoldability );
    }


    @Override
    public PreparedStatement prepareStatement( String sql ) throws SQLException {
        throwIfClosed();
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        if (hasRunningTransaction) {
            commit();
        }
        this.isAutoCommit = autoCommit;
    }


    @Override
    public boolean getAutoCommit() throws SQLException {
        throwIfClosed();
        return isAutoCommit;

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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setReadOnly( boolean readOnly ) throws SQLException {
        throwIfClosed();
        throwIfRunningTransaction();
        isReadOnly = readOnly;
    }


    @Override
    public boolean isReadOnly() throws SQLException {
        throwIfClosed();
        return isReadOnly;

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
        if (!ValidPropertyValues.isValidIsolationLevel( level )) {
            throw new SQLException("Illeagal argument for transaciton isolation level");
        }
        transactionIsolation = level;
    }


    @Override
    public int getTransactionIsolation() throws SQLException {
        throwIfClosed();
        return transactionIsolation;

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
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency );
        return new PolyphenyStatement( this, resultSetType, resultSetConcurrency, resultSetHoldability);

    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency );
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency ) throws SQLException {
        throwIfClosed();
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency );

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
        if (!ValidPropertyValues.isValidResultSetHoldability( holdability )) {
            throw new SQLException("Illegal argument for result set holdability");
        }
        resultSetHoldability = holdability;
    }


    @Override
    public int getHoldability() throws SQLException {
        throwIfClosed();
        return resultSetHoldability;

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
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        return new PolyphenyStatement( this, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    @Override
    public PreparedStatement prepareStatement( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public CallableStatement prepareCall( String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfClosed();
        ValidPropertyValues.throwIfOneInvalid( resultSetType, resultSetConcurrency, resultSetHoldability );

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
        if (!ValidPropertyValues.isValidAutogeneratedKeys( autoGeneratedKeys )) {
            throw new SQLException("Illegal argument for autogenerated keys");
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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Blob createBlob() throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public NClob createNClob() throws SQLException {
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        if (timeout < 0) {
            throw new SQLException("Illegal argument for timeout");
        }
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        if (milliseconds < 0) {
            throw new SQLException("Illegal argument for timeout");
        }
        if (executor == null) {
            throw new SQLException("Executor must not be null");
        }

        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getNetworkTimeout() throws SQLException {
        throwIfClosed();
        return networkTimeout;

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

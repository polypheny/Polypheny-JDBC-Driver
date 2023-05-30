package org.polypheny.jdbc;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import lombok.SneakyThrows;

public class PolyphenyConnection implements Connection {
    private ProtoInterfaceClient protoInterfaceClient;
    public PolyphenyConnection(ProtoInterfaceClient protoInterfaceClient) {
        this.protoInterfaceClient = protoInterfaceClient;
    }

    public ProtoInterfaceClient getProtoInterfaceClient() {
        return protoInterfaceClient;
    }

    @Override
    public Statement createStatement() throws SQLException {
        return new PolyphenyStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void commit() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void rollback() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void close() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public String getCatalog() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void clearWarnings() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @SneakyThrows
    @Override
    public void setClientInfo(String s, String s1) throws SQLClientInfoException {
        throw new SQLException("Feature not implemented");
    }

    @SneakyThrows
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setSchema(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public String getSchema() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        throw new SQLException("Feature not implemented");
    }
}

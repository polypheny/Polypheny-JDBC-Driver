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
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public CallableStatement prepareCall(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public String nativeSQL(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setAutoCommit(boolean b) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void commit() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void rollback() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void close() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setReadOnly(boolean b) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setCatalog(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public String getCatalog() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setTransactionIsolation(int i) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void clearWarnings() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public Statement createStatement(int i, int i1) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void setHoldability(int i) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getHoldability() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Savepoint setSavepoint(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public Statement createStatement(int i, int i1, int i2) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1, int i2) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1, int i2) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Clob createClob() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Blob createBlob() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public NClob createNClob() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean isValid(int i) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @SneakyThrows
    @Override
    public void setClientInfo(String s, String s1) throws SQLClientInfoException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @SneakyThrows
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public String getClientInfo(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Array createArrayOf(String s, Object[] objects) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Struct createStruct(String s, Object[] objects) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setSchema(String s) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public String getSchema() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void setNetworkTimeout(Executor executor, int i) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }
}

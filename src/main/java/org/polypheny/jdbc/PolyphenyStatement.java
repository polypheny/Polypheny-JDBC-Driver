package org.polypheny.jdbc;

import org.polypheny.jdbc.proto.QueryResult;

import java.sql.*;

public class PolyphenyStatement implements Statement {
    private PolyphenyConnection connection;

    public PolyphenyStatement(PolyphenyConnection connection) {
        this.connection = connection;
    }

    @Override
    public ResultSet executeQuery(String s) throws SQLException {
        return null;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        QueryResult result = connection.getProtoInterfaceClient().executeSimpleQuery(sql);
        if (result.getResultCase() == QueryResult.ResultCase.FRAME) {
            throw new SQLException("Statement produces a result set.");
        }
        if (result.getResultCase() == QueryResult.ResultCase.BIGCOUNT) {
            throw new SQLException("Row count to large for return type int.");
        }
        return result.getResultCase() == QueryResult.ResultCase.NORESULT ? 0 : result.getCount();
    }

    @Override
    public void close() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void cancel() throws SQLException {
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
    public void setCursorName(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean execute(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getUpdateCount() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getResultSetType() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void addBatch(String s) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isClosed() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLException("Feature not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
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

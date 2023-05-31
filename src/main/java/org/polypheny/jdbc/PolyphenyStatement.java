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
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        /*
        QueryResult result = connection.getProtoInterfaceClient().executeSimpleQuery(sql);
        if (result.getResultCase() == QueryResult.ResultCase.FRAME) {
            throw new SQLException("Statement produces a result set.");
        }
        if (result.getResultCase() == QueryResult.ResultCase.BIGCOUNT) {
            throw new SQLException("Row count to large for return type int.");
        }
        return result.getResultCase() == QueryResult.ResultCase.NORESULT ? 0 : result.getCount();
        */
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    private void throwNotImplemented() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void close() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setMaxFieldSize(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getMaxRows() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setMaxRows(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void setEscapeProcessing(boolean b) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setQueryTimeout(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void cancel() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void clearWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void setCursorName(String s) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean execute(String s) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int getUpdateCount() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setFetchDirection(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getFetchDirection() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setFetchSize(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int getFetchSize() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int getResultSetType() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void addBatch(String s) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public void clearBatch() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int executeUpdate(String s, int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int executeUpdate(String s, int[] ints) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int executeUpdate(String s, String[] strings) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean execute(String s, int i) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean isClosed() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void setPoolable(boolean b) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean isPoolable() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {}
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException("Feature " + methodName + " not implemented");
        
    }
}

package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
import lombok.Getter;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.StatementBatchStatus;
import org.polypheny.jdbc.proto.StatementStatus;
import org.polypheny.jdbc.utils.CallbackQueue;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class PolyphenyStatement implements Statement {

    private PolyphenyConnection polyphenyConnection;
    protected ResultSet currentResult;
    protected long currentUpdateCount;
    @Getter
    protected int statementId;

    private boolean isClosed;
    private boolean isClosedOnCompletion;
    protected PolyphenyStatementProperties properties;

    // Value used to represent that no value is set for the update count according to JDBC.
    protected static final int NO_UPDATE_COUNT = -1;
    protected static final int NO_STATEMENT_ID = -1;

    protected List<String> statementBatch;


    public PolyphenyStatement(PolyphenyConnection connection, PolyphenyStatementProperties properties) throws SQLException {
        this.polyphenyConnection = connection;
        this.properties = properties;
        this.isClosed = false;
        this.isClosedOnCompletion = false;
        this.statementBatch = new LinkedList<>();
        this.properties.setPolyphenyStatement(this);
        this.statementId = NO_STATEMENT_ID;
        this.currentResult = null;
    }


    protected ResultSet createResultSet(Frame frame) throws SQLException {
        switch (properties.getResultSetType()) {
            case ResultSet.TYPE_FORWARD_ONLY:
                return new PolyphenyForwardResultSet(this, frame, properties.toResultSetProperties());
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return new PolyphenyBidirectionalResultSet(this, frame, properties.toResultSetProperties());
            default:
                throw new SQLException("Should never be thrown");
        }
    }

    public boolean hasStatementId() {
        return statementId != NO_STATEMENT_ID;
    }

    protected ProtoInterfaceClient getClient() {
        return polyphenyConnection.getProtoInterfaceClient();
    }


    protected int longToInt(long longNumber) {
        return Math.toIntExact(longNumber);
    }


    protected void closeCurrentResult() throws SQLException {
        currentResult.close();
        if (statementId != NO_STATEMENT_ID) {
            getClient().closeStatement(statementId);
        }
        currentResult = null;
        currentUpdateCount = NO_UPDATE_COUNT;
    }


    void discardStatementId() {
        statementId = NO_STATEMENT_ID;
    }


    protected void throwIfClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("Illegal operation for a closed statement");
        }
    }


    protected void throwIfNotRelational(Frame frame) throws SQLException {
        if (frame.getResultCase() == ResultCase.RELATIONAL_FRAME) {
            return;
        }
        throw new SQLException("Statement must produce a relational result");
    }


    @Override
    public ResultSet executeQuery(String statement) throws SQLException {
        throwIfClosed();
        closeCurrentResult();
        discardStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement(properties, statement, callback);
            while (true) {
                StatementStatus status = callback.takeNext();
                if (!hasStatementId()) {
                    statementId = status.getStatementId();
                }
                if (!status.hasResult()) {
                    continue;
                }
                callback.awaitCompletion();
                if (!status.getResult().hasFrame()) {
                    throw new SQLException("Statement must produce a single ResultSet");
                }
                Frame frame = status.getResult().getFrame();
                throwIfNotRelational(frame);
                currentResult = createResultSet(frame);
                return currentResult;
            }
        } catch (StatusRuntimeException | InterruptedException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public int executeUpdate(String statement) throws SQLException {
        throwIfClosed();
        closeCurrentResult();
        discardStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement(properties, statement, callback);
            while (true) {
                StatementStatus status = callback.takeNext();
                if (!hasStatementId()) {
                    statementId = status.getStatementId();
                }
                if (!status.hasResult()) {
                    continue;
                }
                callback.awaitCompletion();
                if (status.getResult().hasFrame()) {
                    throw new SQLException("Statement must not produce a ResultSet");
                }
                currentUpdateCount = status.getResult().getScalar();
                return longToInt(currentUpdateCount);
            }
        } catch (StatusRuntimeException | InterruptedException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public void close() throws SQLException {
        if (currentResult != null) {
            currentResult.close();
        }
        isClosed = true;
        polyphenyConnection.removeStatementFromOpen(this);
        getClient().closeStatement(statementId);
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        throwIfClosed();
        return properties.getMaxFieldSize();
    }


    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        throwIfClosed();
        properties.setMaxFieldSize(max);
    }


    @Override
    public long getLargeMaxRows() throws SQLException {
        throwIfClosed();
        return properties.getLargeMaxRows();
    }


    @Override
    public int getMaxRows() throws SQLException {
        throwIfClosed();
        return longToInt(getLargeMaxRows());
    }


    @Override
    public void setLargeMaxRows(long max) throws SQLException {
        throwIfClosed();
        properties.setLargeMaxRows(max);
    }


    @Override
    public void setMaxRows(int max) throws SQLException {
        setLargeMaxRows(max);
    }


    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        throwIfClosed();
        properties.setDoesEscapeProcessing(enable);
    }


    @Override
    public int getQueryTimeout() throws SQLException {
        throwIfClosed();
        return properties.getQueryTimeoutSeconds();
    }


    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        throwIfClosed();
        if (seconds < 0) {
            throw new SQLException("Illegal argument for max");
        }
        properties.setQueryTimeoutSeconds(seconds);
    }


    @Override
    public void cancel() throws SQLException {
        throwIfClosed();
        // TODO TH: implment cancelling
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;

    }


    @Override
    public void clearWarnings() throws SQLException {
        throwIfClosed();
    }


    @Override
    public void setCursorName(String s) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public boolean execute(String statement) throws SQLException {
        throwIfClosed();
        closeCurrentResult();
        discardStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement(properties, statement, callback);
            while (true) {
                StatementStatus status = callback.takeNext();
                if (!hasStatementId()) {
                    statementId = status.getStatementId();
                }
                if (!status.hasResult()) {
                    continue;
                }
                callback.awaitCompletion();
                if (!status.getResult().hasFrame()) {
                    currentUpdateCount = longToInt(status.getResult().getScalar());
                    return false;
                }
                Frame frame = status.getResult().getFrame();
                throwIfNotRelational(frame);
                currentResult = createResultSet(frame);
                return true;

            }
        } catch (ProtoInterfaceServiceException | InterruptedException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        throwIfClosed();
        return currentResult;
    }


    @Override
    public long getLargeUpdateCount() throws SQLException {
        throwIfClosed();
        return currentUpdateCount;
    }


    @Override
    public int getUpdateCount() throws SQLException {
        return longToInt(getLargeUpdateCount());
    }


    @Override
    public boolean getMoreResults() throws SQLException {
        throwIfClosed();
        closeCurrentResult();
        // statements can not return multiple result sets
        return false;
    }


    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throwIfClosed();
        properties.setFetchDirection(direction);
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return properties.getFetchDirection();
    }


    @Override
    public void setFetchSize(int rows) throws SQLException {
        throwIfClosed();
        properties.setFetchSize(rows);
    }


    @Override
    public int getFetchSize() throws SQLException {
        throwIfClosed();
        return properties.getFetchSize();
    }


    @Override
    public int getResultSetConcurrency() throws SQLException {
        throwIfClosed();
        return properties.getResultSetConcurrency();
    }


    @Override
    public int getResultSetType() throws SQLException {
        throwIfClosed();
        return properties.getResultSetType();
    }


    @Override
    public void addBatch(String sql) throws SQLException {
        throwIfClosed();
        statementBatch.add(sql);
    }


    @Override
    public void clearBatch() throws SQLException {
        statementBatch.clear();
    }


    @Override
    public long[] executeLargeBatch() throws SQLException {
        List<Long> scalars = executeUnparameterizedBatch();
        long[] updateCounts = new long[scalars.size()];
        for (int i = 0; i < scalars.size(); i++) {
            updateCounts[i] = scalars.get(i);
        }
        return updateCounts;
    }


    @Override
    public int[] executeBatch() throws SQLException {
        List<Long> scalars = executeUnparameterizedBatch();
        int[] updateCounts = new int[scalars.size()];
        for (int i = 0; i < scalars.size(); i++) {
            updateCounts[i] = longToInt(scalars.get(i));
        }
        return updateCounts;
    }


    private List<Long> executeUnparameterizedBatch() throws SQLException {
        throwIfClosed();
        closeCurrentResult();
        discardStatementId();
        CallbackQueue<StatementBatchStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatementBatch(properties, statementBatch, callback);
            while (true) {
                StatementBatchStatus status = callback.takeNext();
                if (!hasStatementId()) {
                    statementId = status.getBatchId();
                }
                if (status.getScalarsCount() == 0) {
                    continue;
                }
                callback.awaitCompletion();
                return status.getScalarsList();
            }
        } catch (ProtoInterfaceServiceException | InterruptedException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public Connection getConnection() throws SQLException {
        throwIfClosed();
        return polyphenyConnection;
    }


    @Override
    public boolean getMoreResults(int i) throws SQLException {
        if (i == KEEP_CURRENT_RESULT || i == CLOSE_ALL_RESULTS) {
            throw new SQLFeatureNotSupportedException();
        }
        throwIfClosed();
        closeCurrentResult();
        // statements can not return multiple result sets
        return false;
    }


    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public long executeLargeUpdate(String sql, int autogeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate(String sql, int autogeneratedKeys) throws SQLException {
        return longToInt(executeLargeUpdate(sql, autogeneratedKeys));
    }


    @Override
    public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return longToInt(executeLargeUpdate(sql, columnIndexes));
    }


    @Override
    public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return longToInt(executeLargeUpdate(sql, columnNames));
    }


    @Override
    public boolean execute(String s, int i) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }


    @Override
    public boolean execute(String s, int[] ints) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }


    @Override
    public boolean execute(String s, String[] strings) throws SQLException {
        throw new SQLFeatureNotSupportedException();

    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        throwIfClosed();
        return properties.getResultSetHoldability();
    }


    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }


    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throwIfClosed();
        properties.setIsPoolable(poolable);
    }


    @Override
    public boolean isPoolable() throws SQLException {
        throwIfClosed();
        return properties.isPoolable();
    }


    @Override
    public void closeOnCompletion() throws SQLException {
        throwIfClosed();
        isClosedOnCompletion = true;
    }


    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        throwIfClosed();
        return isClosedOnCompletion;
    }


    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new SQLException("Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) throws SQLException {
        return aClass.isInstance(this);

    }


}

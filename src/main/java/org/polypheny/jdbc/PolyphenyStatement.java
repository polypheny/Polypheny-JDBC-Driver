package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import lombok.Getter;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.StatementStatus;
import org.polypheny.jdbc.utils.DefaultPropertyValues;
import org.polypheny.jdbc.utils.StatementStatusQueue;
import org.polypheny.jdbc.utils.ValidPropertyValues;

public class PolyphenyStatement implements Statement {

    private PolyphenyConnection polyphenyConnection;
    private ResultSet currentResult;
    private int currentUpdateCount;
    @Getter
    private int statementId;

    private int queryTimeoutSeconds;
    private int resultSetType;
    private int resultSetConcurrency;
    private int resultSetHoldability;
    private int fetchSize;
    private int fetchDirection;
    private int maxFieldSize;
    private int maxRows;
    private long largeMaxRows;
    private boolean doesEscapeProcessing;
    private boolean isPoolable;
    private boolean isClosed;
    private boolean isClosedOnCompletion;

    // Value used to represent that no value is set for the update count according to JDBC.
    private static final int NO_UPDATE_COUNT = -1;
    private static final int NO_STATEMENT_ID = -1;


    public PolyphenyStatement( PolyphenyConnection connection, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        this.polyphenyConnection = connection;
        this.resultSetType = resultSetType;
        this.resultSetConcurrency = resultSetConcurrency;
        this.resultSetHoldability = resultSetHoldability;
        this.queryTimeoutSeconds = DefaultPropertyValues.getQUERY_TIMEOUT_SECONDS();
        this.fetchSize = DefaultPropertyValues.getFETCH_SIZE();
        this.fetchDirection = DefaultPropertyValues.getFETCH_DIRECTION();
        this.maxFieldSize = DefaultPropertyValues.getMAX_FIELD_SIZE();
        this.maxRows = DefaultPropertyValues.getMAX_ROWS();
        this.largeMaxRows = DefaultPropertyValues.getLARGE_MAX_ROWS();
        this.doesEscapeProcessing = DefaultPropertyValues.isDOING_ESCAPE_PROCESSING();
        this.isPoolable = DefaultPropertyValues.isSTATEMENT_POOLABLE();
        this.isClosed = false;
        this.isClosedOnCompletion = false;
        resetCurrentResults();
        resetStatementId();
    }


    ProtoInterfaceClient getClient() {
        return polyphenyConnection.getProtoInterfaceClient();
    }


    private int longToInt( long longNumber ) {
        return Math.toIntExact( longNumber );
    }


    private void resetCurrentResults() {
        currentResult = null;
        currentUpdateCount = NO_UPDATE_COUNT;
    }


    private void resetStatementId() {
        statementId = NO_STATEMENT_ID;
    }


    private void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "Illegal operation for a closed statement" );
        }
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        resetStatementId();
        StatementStatusQueue callback = new StatementStatusQueue();
        try {
            getClient().executeUnparameterizedStatement( statement, callback );
            while ( true ) {
                StatementStatus status = callback.takeNext();
                if ( statementId == NO_STATEMENT_ID ) {
                    statementId = status.getStatementId();
                }
                if ( !status.hasResult() ) {
                    continue;
                }
                callback.awaitCompletion();
                resetCurrentResults();
                if ( !status.getResult().hasFrame() ) {
                    throw new SQLException( "Statement must produce a single ResultSet" );
                }
                if ( status.getResult().getFrame().getResultCase() != ResultCase.RELATIONAL_FRAME ) {
                    throw new SQLException( "Statement must produce a relational result" );
                }
                currentResult = new PolyphenyResultSet( this, status.getResult().getFrame() );
                return currentResult;
            }
        } catch ( StatusRuntimeException | InterruptedException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        resetStatementId();
        StatementStatusQueue callback = new StatementStatusQueue();
        try {
            getClient().executeUnparameterizedStatement( statement, callback );
            while ( true ) {
                StatementStatus status = callback.takeNext();
                if ( statementId == NO_STATEMENT_ID ) {
                    statementId = status.getStatementId();
                }
                if ( !status.hasResult() ) {
                    continue;
                }
                callback.awaitCompletion();
                resetCurrentResults();
                if ( status.getResult().hasFrame() ) {
                    throw new SQLException( "Statement must not produce a ResultSet" );
                }
                return longToInt( status.getResult().getScalar() );
            }
        } catch ( StatusRuntimeException | InterruptedException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public void close() throws SQLException {
        getClient().closeStatement( statementId );
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        throwIfClosed();
        return maxFieldSize;
    }


    @Override
    public void setMaxFieldSize( int max ) throws SQLException {
        throwIfClosed();
        if ( max < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        maxFieldSize = max;
    }


    @Override
    public int getMaxRows() throws SQLException {
        throwIfClosed();
        return maxRows;
    }


    @Override
    public void setMaxRows( int max ) throws SQLException {
        throwIfClosed();
        if ( max < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        maxRows = max;
    }

    @Override
    public void setEscapeProcessing( boolean enable ) throws SQLException {
        throwIfClosed();
        doesEscapeProcessing = enable;
    }


    @Override
    public int getQueryTimeout() throws SQLException {
        throwIfClosed();
        return queryTimeoutSeconds;
    }


    @Override
    public void setQueryTimeout( int seconds ) throws SQLException {
        throwIfClosed();
        if ( seconds < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        queryTimeoutSeconds = seconds;
    }


    @Override
    public void cancel() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void clearWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void setCursorName( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean execute( String statement ) throws SQLException {
        resetStatementId();
        System.out.println( "call to execute()" );
        StatementStatusQueue callback = new StatementStatusQueue();
        try {
            getClient().executeUnparameterizedStatement( statement, callback );
            while ( true ) {
                StatementStatus status = callback.takeNext();
                if ( statementId == NO_STATEMENT_ID ) {
                    statementId = status.getStatementId();
                }
                if ( !status.hasResult() ) {
                    continue;
                }
                callback.awaitCompletion();
                resetCurrentResults();
                if ( status.getResult().hasFrame() ) {
                    currentResult = new PolyphenyResultSet( this, status.getResult().getFrame() );
                    return true;
                }
                currentUpdateCount = longToInt( status.getResult().getScalar() );
                return false;
            }
        } catch ( ProtoInterfaceServiceException | InterruptedException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResult;
    }


    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }


    @Override
    public boolean getMoreResults() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setFetchDirection( int direction ) throws SQLException {
        throwIfClosed();
        if ( ValidPropertyValues.isInvalidFetchDdirection( direction ) ) {
            throw new SQLException( "Illegal argument for direction" );
        }
        fetchDirection = direction;
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return fetchDirection;
    }


    @Override
    public void setFetchSize( int rows ) throws SQLException {
        throwIfClosed();
        if ( fetchDirection < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        fetchSize = rows;
    }


    @Override
    public int getFetchSize() throws SQLException {
        throwIfClosed();
        return fetchSize;
    }


    @Override
    public int getResultSetConcurrency() throws SQLException {
        throwIfClosed();
        return resultSetConcurrency;
    }


    @Override
    public int getResultSetType() throws SQLException {
        throwIfClosed();
        return resultSetType;
    }


    @Override
    public void addBatch( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void clearBatch() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int[] executeBatch() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public Connection getConnection() throws SQLException {
        return polyphenyConnection;
    }


    @Override
    public boolean getMoreResults( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public int executeUpdate( String s, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public int executeUpdate( String s, int[] ints ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public int executeUpdate( String s, String[] strings ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean execute( String s, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean execute( String s, int[] ints ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean execute( String s, String[] strings ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        throwIfClosed();
        return resultSetHoldability;
    }


    @Override
    public boolean isClosed() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setPoolable( boolean poolable ) throws SQLException {
        throwIfClosed();
        isPoolable = poolable;
    }


    @Override
    public boolean isPoolable() throws SQLException {
        throwIfClosed();
        return isPoolable;
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
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


}

package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.StatementBatchStatus;
import org.polypheny.jdbc.proto.StatementStatus;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.jdbc.utils.ValidPropertyValues;

public class PolyphenyStatement implements Statement {

    private PolyphenyConnection polyphenyConnection;
    private ResultSet currentResult;
    private int currentUpdateCount;
    @Getter
    private int statementId;

    private boolean isClosed;
    private boolean isClosedOnCompletion;
    StatementProperties properties;

    // Value used to represent that no value is set for the update count according to JDBC.
    private static final int NO_UPDATE_COUNT = -1;
    private static final int NO_STATEMENT_ID = -1;

    List<String> statementBatch;

    public PolyphenyStatement( PolyphenyConnection connection, StatementProperties properties ) {
        this.polyphenyConnection = connection;
        this.properties = properties;

        this.isClosed = false;
        this.isClosedOnCompletion = false;
        this.statementBatch = new LinkedList<>();
        resetCurrentResults();
        resetStatementId();
    }


    private ResultSet createResultSet( Frame frame ) throws SQLException {
        switch ( properties.getResultSetType() ) {
            case ResultSet.TYPE_FORWARD_ONLY:
                return new PolyphenyForwardResultSet( this, frame, properties.toResultSetProperties() );
            case ResultSet.TYPE_SCROLL_INSENSITIVE:
            case ResultSet.TYPE_SCROLL_SENSITIVE:
                return new PolyphenyBidirectionalResultSet( this, frame, properties.toResultSetProperties() );
            default:
                throw new SQLException( "Should never be thrown" );
        }
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
        throwIfClosed();
        resetStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement( statement, callback);
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
                Frame frame = status.getResult().getFrame();
                currentResult = createResultSet( frame );
                return currentResult;
            }
        } catch ( StatusRuntimeException | InterruptedException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        throwIfClosed();
        resetStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement( statement, callback);
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
        return properties.getMaxFieldSize();
    }


    @Override
    public void setMaxFieldSize( int max ) throws SQLException {
        throwIfClosed();
        if ( max < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        properties.setMaxFieldSize( max );
    }


    @Override
    public int getMaxRows() throws SQLException {
        throwIfClosed();
        return properties.getMaxRows();
    }


    @Override
    public void setMaxRows( int max ) throws SQLException {
        throwIfClosed();
        if ( max < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        properties.setMaxRows( max );
    }


    @Override
    public void setEscapeProcessing( boolean enable ) throws SQLException {
        throwIfClosed();
        properties.setDoesEscapeProcessing( enable );
    }


    @Override
    public int getQueryTimeout() throws SQLException {
        throwIfClosed();
        return properties.getQueryTimeoutSeconds();
    }


    @Override
    public void setQueryTimeout( int seconds ) throws SQLException {
        throwIfClosed();
        if ( seconds < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        properties.setQueryTimeoutSeconds( seconds );
    }


    @Override
    public void cancel() throws SQLException {
        throwIfClosed();
        // TODO TH: implment cancelling
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
        resetStatementId();
        CallbackQueue<StatementStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatement( statement, callback);
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
                Frame frame = status.getResult().getFrame();
                if ( status.getResult().hasFrame() ) {
                    currentResult = createResultSet( frame );
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
        throwIfClosed();
        return currentResult;
    }


    @Override
    public int getUpdateCount() throws SQLException {
        throwIfClosed();
        return currentUpdateCount;
    }


    @Override
    public boolean getMoreResults() throws SQLException {
        throwIfClosed();
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
        properties.setFetchDirection( direction );
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return properties.getFetchDirection();
    }


    @Override
    public void setFetchSize( int rows ) throws SQLException {
        throwIfClosed();
        if ( rows < 0 ) {
            throw new SQLException( "Illegal argument for max" );
        }
        properties.setFetchSize( rows );
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
    public void addBatch( String sql ) throws SQLException {
        throwIfClosed();
        statementBatch.add( sql );
    }


    @Override
    public void clearBatch() throws SQLException {
        statementBatch.clear();
    }


    @Override
    public int[] executeBatch() throws SQLException {
        throwIfClosed();
        resetStatementId();
        CallbackQueue<StatementBatchStatus> callback = new CallbackQueue<>();
        try {
            getClient().executeUnparameterizedStatementBatch( statementBatch, callback );
            while ( true ) {
                StatementBatchStatus status = callback.takeNext();
                if ( statementId == NO_STATEMENT_ID ) {
                    statementId = status.getBatchId();
                }
                if ( !status.hasResults()) {
                    continue;
                }
                callback.awaitCompletion();
                resetCurrentResults();
                List<Long> scalars = status.getResults().getScalarList();
                int[] updateCounts = new int[scalars.size()];
                for (int i = 0; i < scalars.size(); i++) {
                    updateCounts[i] = longToInt( scalars.get( i ) );
                }
                return updateCounts;
            }
        } catch ( ProtoInterfaceServiceException | InterruptedException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public Connection getConnection() throws SQLException {
        throwIfClosed();
        return polyphenyConnection;
    }


    @Override
    public boolean getMoreResults( int i ) throws SQLException {
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        throwIfClosed();
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
        return properties.getResultSetHoldability();
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
        properties.setPoolable( poolable );
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

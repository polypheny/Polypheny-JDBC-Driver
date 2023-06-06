package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import org.polypheny.jdbc.proto.QueryResult;
import org.polypheny.jdbc.proto.QueryResult.ResultCase;

public class PolyphenyStatement implements Statement {

    private PolyphenyConnection connection;
    private ModificationAwareHashMap<String, String> statementProperties;
    private ResultSet currentResult;

    private int statementId;

    private int currentUpdateCount;
    // Value used to represent that no value is set for the update count according to JDBC.
    private static final int NO_UPDATE_COUNT = -1;
    private static final int NO_STATEMENT_ID = -1;


    public PolyphenyStatement( PolyphenyConnection connection ) {
        this.connection = connection;
        this.statementProperties = new ModificationAwareHashMap<>();
        this.currentUpdateCount = NO_UPDATE_COUNT;
        this.statementId = NO_STATEMENT_ID;
    }


    private int longToInt( long longNumber ) {
        return Math.toIntExact( longNumber );
    }


    private void resetCurrentResults() {
        currentResult = null;
        currentUpdateCount = NO_UPDATE_COUNT;
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        // TODO TH: checking mechanism for statement to only produce single result set
        try {
            QueryResult result = connection.getProtoInterfaceClient().executeUnparameterizedStatement( statement, statementProperties );
            statementId = result.getStatementId();
            resetCurrentResults();
            if ( result.getResultCase() != ResultCase.FRAME ) {
                throw new SQLException( "Statement must produce a single ResultSet" );
            }
            currentResult = new PolyphenyResultSet( result.getFrame() );
            return currentResult;
        } catch ( StatusRuntimeException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        // TODO TH: checking if statement produces result before execution
        try {
            QueryResult result = connection.getProtoInterfaceClient().executeUnparameterizedStatement( statement, statementProperties );
            statementId = result.getStatementId();
            resetCurrentResults();
            switch ( result.getResultCase() ) {
                case FRAME:
                    throw new SQLException( "Statement must not produce a ResultSet" );
                case ROW_COUNT:
                    return longToInt( result.getRowCount() );
                case NO_RESULT:
                    return 0;
                default:
                    throw new SQLException( "Received illegal result from database" );
            }
        } catch ( StatusRuntimeException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public void close() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getMaxFieldSize() throws SQLException {
        return Integer.parseInt( statementProperties.get( "maxFieldSize" ) );
    }


    @Override
    public void setMaxFieldSize( int maxFieldSize ) throws SQLException {
        statementProperties.put( "maxFieldSize", String.valueOf( maxFieldSize ) );
    }


    @Override
    public int getMaxRows() throws SQLException {
        return Integer.parseInt( statementProperties.get( "maxRows" ) );
    }


    @Override
    public void setMaxRows( int maxRows ) throws SQLException {
        statementProperties.put( "maxRows", String.valueOf( maxRows ) );
    }


    @Override
    public void setEscapeProcessing( boolean b ) throws SQLException {
        /* TODO TH: local property that does not have to be sent to the server
         * As the topic of escape replacement is all about jdbc this should not be sent to the server.
         */

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getQueryTimeout() throws SQLException {
        // TODO TH: local property that does not have to be sent to the server

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setQueryTimeout( int i ) throws SQLException {
        // TODO TH: local property that does not have to be sent to the server

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
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
        try {
            QueryResult result = connection.getProtoInterfaceClient().executeUnparameterizedStatement( statement, statementProperties );
            statementId = result.getStatementId();
            resetCurrentResults();
            switch ( result.getResultCase() ) {
                case FRAME:
                    currentResult = new PolyphenyResultSet( result.getFrame() );
                    return true;
                case ROW_COUNT:
                    currentUpdateCount = longToInt( result.getRowCount() );
                    return false;
                case NO_RESULT:
                    return false;
            }
            return false;
        } catch ( StatusRuntimeException e ) {
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
    public void setFetchDirection( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getFetchDirection() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void setFetchSize( int fetchSize ) throws SQLException {
        statementProperties.put( "fetchSize", String.valueOf( fetchSize ) );
    }


    @Override
    public int getFetchSize() throws SQLException {
        return Integer.parseInt( statementProperties.get( "fetchSize" ) );
    }


    @Override
    public int getResultSetConcurrency() throws SQLException {
        // TODO TH: local property that does not have to be sent to the server

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public int getResultSetType() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
        return connection;
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
        // TODO TH: local property that does not have to be sent to the server

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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
    public void setPoolable( boolean b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isPoolable() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

    }


    @Override
    public void closeOnCompletion() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }
                .getClass()
                .getEnclosingMethod()
                .getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );

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

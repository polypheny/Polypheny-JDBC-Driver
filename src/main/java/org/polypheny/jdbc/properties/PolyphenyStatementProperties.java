package org.polypheny.jdbc.properties;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.PolyphenyStatement;
import org.polypheny.jdbc.ProtoInterfaceClient;

import java.sql.SQLException;

public class PolyphenyStatementProperties {
    private static final int UNSET_INT = -1;

    @Setter
    ProtoInterfaceClient protoInterfaceClient;
    PolyphenyStatement polyphenyStatement;
    @Getter
    private int queryTimeoutSeconds;
    @Getter
    private int resultSetType = UNSET_INT;
    @Getter
    private int resultSetConcurrency = UNSET_INT;
    @Getter
    private int resultSetHoldability = UNSET_INT;
    @Getter
    private int fetchSize;
    @Getter
    private int fetchDirection;
    @Getter
    private int maxFieldSize;
    @Getter
    private long largeMaxRows;
    @Getter
    private boolean doesEscapeProcessing;
    @Getter
    private boolean isPoolable;

    public void setPolyphenyStatement (PolyphenyStatement polyphenyStatement) throws SQLException {
        if (this.polyphenyStatement != null) {
            throw new SQLException("Can't change polyphenyStatement" + polyphenyStatement);
        }
        this.polyphenyStatement = polyphenyStatement;
    }

    public void setQueryTimeoutSeconds(int queryTimeoutSeconds) throws SQLException {
        if ( queryTimeoutSeconds < 0 ) {
            throw new SQLException( "Illegal value for max" );
        }
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        syncIfStatementPresent();

    }

    public void setResultSetType(int resultSetType) throws SQLException {
        if (this.resultSetType != UNSET_INT) {
            throw new SQLException("Can't change result set type");
        }
        if (!PropertyUtils.isValidResultSetType(resultSetType)) {
            throw new SQLException("Illegal value for result set type");
        }
        this.resultSetType = resultSetType;
    }

    public void setResultSetConcurrency(int resultSetConcurrency) throws SQLException {
        if (this.resultSetConcurrency != UNSET_INT) {
            throw new SQLException("Can't change result set type");
        }
        if (!PropertyUtils.isValidResultSetConcurrency(resultSetConcurrency)) {
            throw new SQLException("Illegal value for result set concurrency");
        }
        this.resultSetConcurrency = resultSetConcurrency;
        syncIfStatementPresent();
    }

    public void setResultSetHoldability(int resultSetHoldability) throws SQLException {
        if (this.resultSetHoldability != UNSET_INT) {
            throw new SQLException("Can't change result set type");
        }
        if (!PropertyUtils.isValidResultSetHoldability(resultSetHoldability)) {
            throw new SQLException("Illegal value for result set concurrency");
        }
        this.resultSetHoldability = resultSetHoldability;
        // not transmitted to server -> no sync()
    }

    public void setFetchSize(int fetchSize) throws SQLException {
        if ( fetchSize < 0 ) {
            throw new SQLException( "Illegal value for fetch size" );
        }
        this.fetchSize = fetchSize;
        syncIfStatementPresent();
    }

    public void setFetchDirection(int fetchDirection) throws SQLException {
        if ( PropertyUtils.isInvalidFetchDdirection( fetchDirection ) ) {
            throw new SQLException( "Illegal value for fetch direction" );
        }
        this.fetchDirection = fetchDirection;
        syncIfStatementPresent();
    }

    public void setMaxFieldSize(int maxFieldSize) throws SQLException {
        if ( maxFieldSize < 0 ) {
            throw new SQLException( "Illegal argument for max field size" );
        }
        this.maxFieldSize = maxFieldSize;
        syncIfStatementPresent();
    }

    public void setLargeMaxRows(long largeMaxRows) throws SQLException {
        if ( largeMaxRows < 0 ) {
            throw new SQLException( "Illegal value for large max rows" );
        }
        this.largeMaxRows = largeMaxRows;
        syncIfStatementPresent();
    }

    public void setDoesEscapeProcessing(boolean doesEscapeProcessing) {
        this.doesEscapeProcessing = doesEscapeProcessing;
        syncIfStatementPresent();
    }

    public void setIsPoolable(boolean isPoolable) {
        this.isPoolable = isPoolable;
        syncIfStatementPresent();
    }


    private void syncIfStatementPresent(){
        if (polyphenyStatement == null) {
            // bypass sync during construction
            return;
        }
        if (!polyphenyStatement.hasStatementId()) {
            // no statement on serverside that could hold the properties
            return;
        }
        protoInterfaceClient.setStatementProperties(this, polyphenyStatement.getStatementId());
    }

    public ResultSetProperties toResultSetProperties() {
        ResultSetProperties properties = new ResultSetProperties();
        properties.setResultSetType(resultSetType);
        properties.setResultSetConcurrency(resultSetConcurrency);
        properties.setResultSetHoldability(resultSetHoldability);
        properties.setFetchDirection(fetchDirection);
        properties.setFetchSize(fetchSize);
        properties.setMaxFieldSize(maxFieldSize);
        properties.setLargeMaxRows(largeMaxRows);
        return properties;
    }
}

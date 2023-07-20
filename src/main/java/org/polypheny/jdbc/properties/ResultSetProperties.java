package org.polypheny.jdbc.properties;

import java.sql.ResultSet;
import lombok.Getter;
import lombok.Setter;

public class ResultSetProperties {

    @Getter
    @Setter
    private int resultSetType;
    @Getter
    @Setter
    private int resultSetConcurrency;
    @Getter
    @Setter
    private int resultSetHoldability;
    @Getter
    @Setter
    private int fetchDirection;
    @Getter
    @Setter
    private int fetchSize;
    @Getter
    @Setter
    private int maxFieldSize;
    @Getter
    @Setter
    private long largeMaxRows;

    public boolean isReadOnly() {
        return resultSetConcurrency == ResultSet.CONCUR_READ_ONLY;
    }

    public static ResultSetProperties forMetaResultSet() {
        ResultSetProperties properties = new ResultSetProperties();
        properties.setResultSetType( ResultSet.TYPE_SCROLL_INSENSITIVE );
        properties.setResultSetConcurrency( ResultSet.CONCUR_READ_ONLY);
        properties.setResultSetHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        properties.setFetchDirection( ResultSet.FETCH_FORWARD );
        properties.setFetchSize( 0 );
        properties.setMaxFieldSize( 0 );
        properties.setLargeMaxRows( 0 );
        return properties;
    }
}

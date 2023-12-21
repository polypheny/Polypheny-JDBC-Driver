package org.polypheny.jdbc.properties;

import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import lombok.Getter;
import lombok.Setter;

public class PolyphenyResultSetProperties {

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
    private int statementFetchSize;
    private int resultSetFetchSize;
    @Getter
    @Setter
    private int maxFieldSize;
    @Getter
    @Setter
    private long largeMaxRows;

    @Getter
    @Setter
    private Calendar calendar;


    public boolean isReadOnly() {
        return resultSetConcurrency == ResultSet.CONCUR_READ_ONLY;
    }


    public static PolyphenyResultSetProperties forMetaResultSet() {
        PolyphenyResultSetProperties properties = new PolyphenyResultSetProperties();
        properties.setResultSetType( ResultSet.TYPE_SCROLL_INSENSITIVE );
        properties.setResultSetConcurrency( ResultSet.CONCUR_READ_ONLY );
        properties.setResultSetHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        properties.setFetchDirection( ResultSet.FETCH_FORWARD );
        properties.setStatementFetchSize( 0 );
        properties.setMaxFieldSize( 0 );
        properties.setLargeMaxRows( 0 );
        properties.setCalendar( Calendar.getInstance( TimeZone.getDefault(), Locale.ROOT ) );
        return properties;
    }


    public void setStatementFetchSize( int fetchSize ) {
        statementFetchSize = fetchSize;
        resultSetFetchSize = fetchSize;
    }


    public void setFetchSize( int fetchSize ) {
        resultSetFetchSize = fetchSize;
    }


    public int getFetchSize() {
        return resultSetFetchSize;
    }

}

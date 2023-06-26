package org.polypheny.jdbc;

import java.sql.SQLException;

public interface BidirectionalScrollable<T> extends Scrollable<T> {

    boolean absolute( int rowIndex ) throws SQLException;

    boolean relative( int offset ) throws SQLException;

    boolean previous() throws SQLException;

    void beforeFirst() throws SQLException;

    void afterLast();

}

package org.polypheny.jdbc;

import java.sql.SQLException;

public interface BidirectionalScrollable<T> extends Scrollable<T> {

    boolean absolute( int rowIndex ) throws ProtoInterfaceServiceException;

    boolean relative( int offset ) throws ProtoInterfaceServiceException;

    boolean previous() throws ProtoInterfaceServiceException;

    void beforeFirst() throws ProtoInterfaceServiceException;

    void afterLast();

    boolean first();

    boolean last() throws InterruptedException;

}

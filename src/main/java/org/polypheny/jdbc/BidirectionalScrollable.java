package org.polypheny.jdbc;

public interface BidirectionalScrollable<T> extends Scrollable<T> {

    boolean absolute( int rowIndex ) throws PrismInterfaceServiceException;

    boolean relative( int offset ) throws PrismInterfaceServiceException;

    boolean previous() throws PrismInterfaceServiceException;

    void beforeFirst() throws PrismInterfaceServiceException;

    void afterLast();

    boolean first();

    boolean last() throws InterruptedException;

}

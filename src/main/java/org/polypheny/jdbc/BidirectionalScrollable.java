package org.polypheny.jdbc;

public interface BidirectionalScrollable<T> extends Scrollable<T> {

    boolean beforeFirst();

    boolean afterLast();

    boolean first();

    boolean last();

    boolean absolute( int rowIndex );

    boolean relative( int offset );

    boolean previous();

}

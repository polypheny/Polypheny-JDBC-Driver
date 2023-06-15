package org.polypheny.jdbc;

public interface Scrollable<T> {

    boolean next();

    T current();

    void close();

    boolean isBeforeFirst();

    boolean isAfterLast();

    boolean isFirst();

    boolean isLast();

    int getRow();

}

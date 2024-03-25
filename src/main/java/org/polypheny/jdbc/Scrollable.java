package org.polypheny.jdbc;

public interface Scrollable<T> {

    void fetchAllAndSync() throws InterruptedException;

    boolean next() throws PrismInterfaceServiceException;

    T current();

    void close();

    boolean isBeforeFirst();

    boolean isAfterLast();

    boolean isFirst();

    boolean isLast();

    int getRow();

    boolean hasCurrent();

}

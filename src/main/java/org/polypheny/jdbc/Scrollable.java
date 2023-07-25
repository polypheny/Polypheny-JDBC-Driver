package org.polypheny.jdbc;

import java.sql.SQLException;

public interface Scrollable<T> {

    boolean next() throws ProtoInterfaceServiceException;

    T current();

    void close();

    boolean isBeforeFirst();

    boolean isAfterLast();

    boolean isFirst();

    boolean isLast();

    int getRow();

}

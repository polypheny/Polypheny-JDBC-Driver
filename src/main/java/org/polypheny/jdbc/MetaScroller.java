package org.polypheny.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;

public class MetaScroller<T> implements BidirectionalScrollable<T> {

    private static final int CURSOR_BEFORE_DATA = -1;

    private final ArrayList<T> data;
    private T current;
    int currentIndex;


    public MetaScroller( ArrayList<T> rows ) {
        this.data = rows;
    }


    private int rowToIndex( int row ) {
        return row - 1;
    }


    private int indexToRow( int index ) {
        return index + 1;
    }


    @Override
    public boolean absolute( int rowIndex ) throws SQLException {
        if ( rowIndex == 0 ) {
            current = null;
            currentIndex = CURSOR_BEFORE_DATA;
            return false;
        }
        if ( rowIndex > 0 ) {
            current = null;
            currentIndex = rowToIndex( rowIndex );
            if ( currentIndex >= data.size() ) {
                currentIndex = data.size();
                return false;
            }
            current = data.get( currentIndex );
            return true;
        }
        current = null;
        currentIndex = data.size() + rowIndex;
        if ( currentIndex > CURSOR_BEFORE_DATA ) {
            current = data.get( currentIndex );
            return true;
        }
        return false;
    }


    @Override
    public boolean relative( int offset ) throws SQLException {
        current = null;
        currentIndex += offset;
        if ( currentIndex < 0 ) {
            currentIndex = CURSOR_BEFORE_DATA;
            return false;
        }
        if ( currentIndex >= data.size() ) {
            currentIndex = data.size();
            return false;
        }
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public boolean previous() throws SQLException {
        current = null;
        currentIndex++;
        if ( currentIndex > CURSOR_BEFORE_DATA ) {
            current = data.get( currentIndex );
            return true;
        }
        currentIndex = CURSOR_BEFORE_DATA;
        return false;
    }


    @Override
    public void beforeFirst() throws SQLException {
        current = null;
        currentIndex = CURSOR_BEFORE_DATA;
    }


    @Override
    public void afterLast() {
        current = null;
        currentIndex = data.size();
    }


    @Override
    public boolean next() throws InterruptedException, SQLException {
        current = null;
        currentIndex++;
        if ( currentIndex >= data.size() ) {
            currentIndex = data.size();
            return false;
        }
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public T current() {
        return current;
    }


    @Override
    public void close() {
        // Used to close any open streams to or from this scrollable. There are none.
    }


    @Override
    public boolean isBeforeFirst() {
        return currentIndex == CURSOR_BEFORE_DATA;
    }


    @Override
    public boolean isAfterLast() {
        return currentIndex >= data.size();
    }


    @Override
    public boolean isFirst() {
        return currentIndex == 0;
    }


    @Override
    public boolean isLast() {
        return currentIndex == data.size() - 1;
    }


    @Override
    public int getRow() {
        return indexToRow( currentIndex );
    }

}

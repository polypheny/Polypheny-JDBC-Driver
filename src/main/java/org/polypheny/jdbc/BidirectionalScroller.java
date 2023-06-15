package org.polypheny.jdbc;

import java.util.ArrayList;
import org.polypheny.jdbc.BidirectionalScrollable;
import org.polypheny.jdbc.types.TypedValue;

public class BidirectionalScroller implements BidirectionalScrollable<ArrayList<TypedValue>> {

    @Override
    public boolean beforeFirst() {
        return false;
    }


    @Override
    public boolean afterLast() {
        return false;
    }


    @Override
    public boolean first() {
        return false;
    }


    @Override
    public boolean last() {
        return false;
    }


    @Override
    public boolean absolute( int rowIndex ) {
        return false;
    }


    @Override
    public boolean relative( int offset ) {
        return false;
    }


    @Override
    public boolean previous() {
        return false;
    }


    @Override
    public boolean next() {
        return false;
    }


    @Override
    public ArrayList<TypedValue> current() {
        return null;
    }


    @Override
    public void close() {

    }


    @Override
    public boolean isBeforeFirst() {
        return false;
    }


    @Override
    public boolean isAfterLast() {
        return false;
    }


    @Override
    public boolean isFirst() {
        return false;
    }


    @Override
    public boolean isLast() {
        return false;
    }


    @Override
    public int getRow() {
        return 0;
    }

}

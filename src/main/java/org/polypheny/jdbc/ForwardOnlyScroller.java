package org.polypheny.jdbc;

/* Disclaimer:
 * This class ended up being basically the same as the IteratorCursor from Avatica.
 * Therefore, I want to express that the idea for this comes from the following link:
 * https://github.com/polypheny/Avatica/blob/polypheny/core/src/main/java/org/apache/calcite/avatica/util/IteratorCursor.java
 */


import java.util.ArrayList;
import org.polypheny.jdbc.types.TypedValue;

public class ForwardOnlyScroller implements Scrollable<ArrayList<TypedValue>> {


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

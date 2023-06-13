package org.polypheny.jdbc;

import java.util.Iterator;
import java.util.NoSuchElementException;

/* Disclaimer:
 * This class ended up being basically the same as the IteratorCursor from Avatica.
 * Therefore, I want to express that the idea for this comes from the following link:
 * https://github.com/polypheny/Avatica/blob/polypheny/core/src/main/java/org/apache/calcite/avatica/util/IteratorCursor.java
 */


public class Cursor<T> {

    private enum CursorPosition {
        BEFORE_FIRST,
        ON_ELEMENT,
        AFTER_LAST,
        CLOSED
    }


    private final Iterator<T> iterator;
    private int frameOffset;
    private CursorPosition cursorPosition;
    private T currentElement;


    public Cursor( Iterator<T> iterator ) {
        this.iterator = iterator;
        this.cursorPosition = CursorPosition.BEFORE_FIRST;
        this.frameOffset = 0;
    }


    public Cursor( Iterator<T> iterator, int frameOffset ) {
        this.iterator = iterator;
        this.cursorPosition = CursorPosition.BEFORE_FIRST;
        this.frameOffset = frameOffset;
    }


    public boolean next() {
        if ( !iterator.hasNext() ) {
            cursorPosition = CursorPosition.CLOSED;
            currentElement = null;
            return false;
        }
        currentElement = iterator.next();
        cursorPosition = CursorPosition.ON_ELEMENT;
        return true;
    }


    public T current() {
        if ( cursorPosition != CursorPosition.ON_ELEMENT ) {
            throw new NoSuchElementException( "Cursor must be pointing on an element." );
        }
        return currentElement;
    }


    public void close() {
        currentElement = null;
        cursorPosition = CursorPosition.CLOSED;
        closeIterator();
    }


    private void closeIterator() {
        if ( iterator instanceof AutoCloseable ) {
            try {
                ((AutoCloseable) iterator).close();
            } catch ( RuntimeException e ) {
                throw e;
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }
    }


    public boolean isBeforeFirst() {
        return cursorPosition == CursorPosition.BEFORE_FIRST;
    }


    public boolean isAfterLast() {
        return cursorPosition == CursorPosition.AFTER_LAST;
    }
}

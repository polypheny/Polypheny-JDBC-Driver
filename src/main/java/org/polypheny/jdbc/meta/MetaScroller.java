/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.jdbc.meta;

import java.util.List;
import org.polypheny.jdbc.BidirectionalScrollable;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class MetaScroller<T> implements BidirectionalScrollable<T> {

    private static final int CURSOR_BEFORE_DATA = -1;

    private final List<T> data;
    private T current;
    private int currentIndex;


    public MetaScroller( List<T> rows ) {
        this.data = rows;
        this.current = null;
        this.currentIndex = CURSOR_BEFORE_DATA;
    }


    private int rowToIndex( int row ) {
        return row - 1;
    }


    private int indexToRow( int index ) {
        return index + 1;
    }


    @Override
    public void fetchAllAndSync() {
    }


    @Override
    public boolean absolute( int rowIndex ) {
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
        return accessFromBack( rowIndex );
    }


    private boolean accessFromBack( int rowIndex ) {
        current = null;
        currentIndex = data.size() + rowIndex;
        if ( currentIndex > CURSOR_BEFORE_DATA ) {
            current = data.get( currentIndex );
            return true;
        }
        currentIndex = CURSOR_BEFORE_DATA;
        return false;
    }


    @Override
    public boolean relative( int offset ) {
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
    public boolean previous() {
        current = null;
        currentIndex--;
        if ( currentIndex > CURSOR_BEFORE_DATA && currentIndex < data.size() ) {
            current = data.get( currentIndex );
            return true;
        }
        currentIndex = CURSOR_BEFORE_DATA;
        return false;
    }


    @Override
    public void beforeFirst() {
        current = null;
        currentIndex = CURSOR_BEFORE_DATA;
    }


    @Override
    public void afterLast() {
        current = null;
        currentIndex = data.size();
    }


    @Override
    public boolean first() {
        current = null;
        currentIndex = CURSOR_BEFORE_DATA;
        if ( data.isEmpty() ) {
            return false;
        }
        currentIndex = 0;
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public boolean last() {
        current = null;
        currentIndex = CURSOR_BEFORE_DATA;
        if ( data.isEmpty() ) {
            return false;
        }
        currentIndex = data.size() - 1;
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public boolean next() throws PrismInterfaceServiceException {
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
        return data.isEmpty() || currentIndex >= data.size();
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
        if ( currentIndex < 0 ) {
            return 0;
        }
        if ( currentIndex >= data.size() ) {
            return 0;
        }
        return indexToRow( currentIndex );
    }


    @Override
    public boolean hasCurrent() {
        return current != null;
    }

}

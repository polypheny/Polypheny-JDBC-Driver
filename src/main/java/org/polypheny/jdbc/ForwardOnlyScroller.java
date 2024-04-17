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

package org.polypheny.jdbc;

import static java.lang.Math.min;

import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ForwardOnlyScroller implements Scrollable<List<TypedValue>> {

    private static final int DEFAULT_PREFETCH_COUNT = 20;
    private static final int INDEX_BEFORE_FIRST = -1;

    private LinkedList<List<TypedValue>> values;
    private List<TypedValue> currentRow;
    private ResultFetcher resultFetcher;
    private Thread fetcherThread;
    private PolyphenyResultSetProperties properties;
    private int baseIndex;


    public ForwardOnlyScroller( Frame frame, PrismInterfaceClient client, int statementId, PolyphenyResultSetProperties properties, int fetchTimeout ) {
        this.values = new LinkedList<>( TypedValueUtils.buildRows( frame.getRelationalFrame().getRowsList() ) );
        if ( properties.getLargeMaxRows() != 0 && values.size() > properties.getLargeMaxRows() ) {
            values.subList( longToInt( properties.getLargeMaxRows() ), values.size() ).clear();
        }
        this.resultFetcher = new ResultFetcher( client, statementId, properties, values.size(), fetchTimeout );
        this.resultFetcher.setLast( frame.getIsLast() );
        this.properties = properties;
        this.baseIndex = INDEX_BEFORE_FIRST;
    }


    protected int longToInt( long longNumber ) {
        return Math.toIntExact( longNumber );
    }


    @Override
    public void fetchAllAndSync() throws InterruptedException {
        if ( resultFetcher.isLast() ) {
            return;
        }
        if ( fetcherThread != null ) {
            return;
        }
        while ( !resultFetcher.isLast() ) {
            fetcherThread = new Thread( resultFetcher );
            fetcherThread.start();
            syncFetch();
        }
    }


    @Override
    public boolean next() throws PrismInterfaceServiceException {
        try {
            considerPrefetch();
            syncFetchIfEmpty();
            currentRow = values.poll();
            if ( currentRow == null ) {
                return false;
            }
            baseIndex++;
            return true;
        } catch ( InterruptedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Fetching more columns from server filed.", e );
        }
    }


    private void considerPrefetch() {
        int prefetch_count = min( DEFAULT_PREFETCH_COUNT, properties.getStatementFetchSize() );
        if ( values.size() > prefetch_count ) {
            return;
        }
        if ( resultFetcher.isLast() ) {
            return;
        }
        if ( fetcherThread != null ) {
            return;
        }
        fetcherThread = new Thread( resultFetcher );
        fetcherThread.start();
    }


    private void syncFetchIfEmpty() throws InterruptedException {
        if ( !values.isEmpty() ) {
            return;
        }
        syncFetch();
    }


    private void syncFetch() throws InterruptedException {
        if ( fetcherThread == null ) {
            return;
        }
        fetcherThread.join();
        fetcherThread = null;
        values.addAll( resultFetcher.getFetchedValues() );
    }


    @Override
    public List<TypedValue> current() {
        if ( currentRow == null ) {
            throw new NoSuchElementException( "Illegal cursor position." );
        }
        return currentRow;
    }


    @Override
    public void close() {
        if ( fetcherThread == null ) {
            return;
        }
        fetcherThread.interrupt();
    }


    @Override
    public boolean isBeforeFirst() {
        return baseIndex == INDEX_BEFORE_FIRST;
    }


    @Override
    public boolean isAfterLast() {
        return values.isEmpty() && currentRow == null;
    }


    @Override
    public boolean isFirst() {
        return baseIndex == 0;
    }


    @Override
    public boolean isLast() {
        return values.isEmpty() && currentRow != null;
    }


    @Override
    public int getRow() {
        if ( isBeforeFirst() || isAfterLast() ) {
            return 0;
        }
        return baseIndex + 1;
    }


    @Override
    public boolean hasCurrent() {
        return currentRow != null;
    }

}

package org.polypheny.jdbc;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ForwardOnlyScroller implements Scrollable<ArrayList<TypedValue>> {

    private static final int DEFAULT_PREFETCH_COUNT = 20;
    private static final int INDEX_BEFORE_FIRST = -1;

    LinkedList<ArrayList<TypedValue>> values;
    ArrayList<TypedValue> currentRow;
    ResultFetcher resultFetcher;
    Thread fetcherThread;
    private int baseIndex;
    private int prefetch_count;


    public ForwardOnlyScroller( Frame frame, ProtoInterfaceClient client, int statementId, int fetchSize ) {
        this.values = new LinkedList<>( TypedValueUtils.buildRows( frame.getRelationalFrame().getRowsList() ) );
        this.resultFetcher = new ResultFetcher( client, statementId, fetchSize );
        this.resultFetcher.setLast( frame.getIsLast() );
        this.baseIndex = INDEX_BEFORE_FIRST;
        this.prefetch_count = DEFAULT_PREFETCH_COUNT;
    }


    @Override
    public boolean next() throws InterruptedException {
        considerPrefetch();
        syncFetch();
        currentRow = values.poll();
        if ( currentRow == null ) {
            return false;
        }
        baseIndex++;
        return true;
    }

    public void setFetchSize(int fetchSize) {
        resultFetcher.setFetchSize( fetchSize );
        prefetch_count = min(DEFAULT_PREFETCH_COUNT, fetchSize);
    }

    public int getFetchSize() {
        return resultFetcher.getFetchSize();
    }

    private void considerPrefetch() {
        if ( values.size() > prefetch_count ) {
            return;
        }
        if ( resultFetcher.isLast() ) {
            return;
        }
        if (fetcherThread != null) {
            return;
        }
        fetcherThread = new Thread( resultFetcher );
        fetcherThread.start();
    }


    private void syncFetch() throws InterruptedException {
        if ( fetcherThread == null ) {
            return;
        }
        if ( !values.isEmpty() ) {
            return;
        }
        fetcherThread.join();
        fetcherThread = null;
        values.addAll( resultFetcher.getFetchedValues() );
    }


    @Override
    public ArrayList<TypedValue> current() {
        if ( currentRow == null ) {
            throw new NoSuchElementException( "Illegal cursor position." );
        }
        return currentRow;
    }


    @Override
    public void close() {

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

}

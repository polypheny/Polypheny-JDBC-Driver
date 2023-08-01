package org.polypheny.jdbc;

import static java.lang.Math.min;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NoSuchElementException;

import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ForwardOnlyScroller implements Scrollable<ArrayList<TypedValue>> {

    private static final int DEFAULT_PREFETCH_COUNT = 20;
    private static final int INDEX_BEFORE_FIRST = -1;

    private LinkedList<ArrayList<TypedValue>> values;
    private ArrayList<TypedValue> currentRow;
    private ResultFetcher resultFetcher;
    private Thread fetcherThread;
    private PolyphenyResultSetProperties properties;
    private int baseIndex;

    public ForwardOnlyScroller( Frame frame, ProtoInterfaceClient client, int statementId, PolyphenyResultSetProperties properties, int fetchTimeout) {
        this.values = new LinkedList<>( TypedValueUtils.buildRows( frame.getRelationalFrame().getRowsList() ) );
        this.resultFetcher = new ResultFetcher( client, statementId, properties, values.size(), fetchTimeout);
        this.resultFetcher.setLast( frame.getIsLast() );
        this.properties = properties;
        this.baseIndex = INDEX_BEFORE_FIRST;
    }


    @Override
    public boolean next() throws ProtoInterfaceServiceException {
        try {
            considerPrefetch();
            syncFetch();
            currentRow = values.poll();
            if ( currentRow == null ) {
                return false;
            }
            baseIndex++;
            return true;
        } catch (InterruptedException e) {
            throw new ProtoInterfaceServiceException(SQLErrors.DRIVER_THREADING_ERROR, "Fetching more columns from server filed.", e);
        }
    }

    private void considerPrefetch() {
        int prefetch_count = min(DEFAULT_PREFETCH_COUNT, properties.getFetchSize());
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

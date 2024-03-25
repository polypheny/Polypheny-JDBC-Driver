package org.polypheny.jdbc;

import static java.lang.Math.min;

import java.util.ArrayList;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.jdbc.jdbctypes.TypedValue;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class BidirectionalScroller implements BidirectionalScrollable<ArrayList<TypedValue>> {

    private static final int INDEX_BEFORE_FIRST = -1;
    private static final int DEFAULT_PREFETCH_COUNT = 20;
    private ArrayList<ArrayList<TypedValue>> values;
    private ArrayList<TypedValue> currentRow;
    private ResultFetcher resultFetcher;
    private PolyphenyResultSetProperties properties;
    private Thread fetcherThread;
    int currentIndex;


    public BidirectionalScroller( Frame frame, PrismInterfaceClient client, int statementId, PolyphenyResultSetProperties properties, int fetchTimeout ) {
        this.values = new ArrayList<>( TypedValueUtils.buildRows( frame.getRelationalFrame().getRowsList() ) );
        if ( properties.getLargeMaxRows() != 0 && values.size() > properties.getLargeMaxRows() ) {
            values.subList( longToInt( properties.getLargeMaxRows() ), values.size() ).clear();
        }
        this.resultFetcher = new ResultFetcher( client, statementId, properties, values.size(), fetchTimeout );
        this.resultFetcher.setLast( frame.getIsLast() );
        this.currentIndex = INDEX_BEFORE_FIRST;
        this.properties = properties;
    }


    protected int longToInt( long longNumber ) {
        return Math.toIntExact( longNumber );
    }


    private boolean fetchUpTo( int rowIndex ) throws InterruptedException {
        while ( values.size() < rowIndex ) {
            if ( resultFetcher.isLast() ) {
                return false;
            }
            fetcherThread = new Thread( resultFetcher );
            fetcherThread.start();
            fetcherThread.join();
        }
        return true;
    }


    @Override
    public void fetchAllAndSync() throws InterruptedException {
        fetchAll();
        syncFetch();
    }


    private void fetchAll() throws InterruptedException {
        while ( !resultFetcher.isLast() ) {
            fetcherThread = new Thread( resultFetcher );
            fetcherThread.start();
            fetcherThread.join();
        }
    }


    @Override
    public boolean absolute( int rowIndex ) throws PrismInterfaceServiceException {
        try {
            if ( rowToIndex( rowIndex ) == currentIndex ) {
                return true;
            }
            if ( rowIndex < 0 ) {
                fetchAll();
                currentIndex = values.size() + rowIndex;
                if ( currentIndex < 1 ) {
                    currentIndex = INDEX_BEFORE_FIRST;
                    currentRow = null;
                    return false;
                }
                currentRow = values.get( currentIndex );
                return true;
            }
            if ( rowIndex == 0 ) {
                currentIndex = INDEX_BEFORE_FIRST;
                currentRow = null;
                return true;
            }
            if ( rowIndex <= values.size() ) {
                currentIndex = rowToIndex( rowIndex );
                currentRow = values.get( currentIndex );
                return true;
            }
            if ( fetchUpTo( rowIndex ) ) {
                currentIndex = rowToIndex( rowIndex );
                currentRow = values.get( currentIndex );
                considerPrefetch();
                return true;
            }
            // Explanation: This is not an off by one error:
            // An index equal to the array size is one position after the last element.
            currentIndex = values.size();
            currentRow = null;
            return false;
        } catch ( InterruptedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Fetching of more rows failed", e );
        }
    }


    private int rowToIndex( int rowIndex ) {
        return rowIndex - 1;
    }


    private int indexToRow( int index ) {
        return index + 1;
    }


    @Override
    public boolean relative( int offset ) throws PrismInterfaceServiceException {
        try {
            if ( offset == 0 ) {
                return currentRow != null;
            }
            if ( currentIndex + offset < 0 ) {
                currentIndex = INDEX_BEFORE_FIRST;
                currentRow = null;
                return false;
            }
            if ( currentIndex + offset < values.size() ) {
                currentIndex += offset;
                currentRow = values.get( currentIndex );
                return true;
            }
            if ( currentIndex + offset >= values.size() ) {
                if ( fetchUpTo( indexToRow( currentIndex + offset ) ) ) {
                    currentIndex += offset;
                    currentRow = values.get( currentIndex );
                    considerPrefetch();
                    return true;
                }
                // Explanation: This is not an off by one error:
                // An index equal to the array size is one position after the last element.
                currentIndex = values.size();
                currentRow = null;
                return false;
            }
        } catch ( InterruptedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Fetching more rows failed.", e );
        }
        throw new PrismInterfaceServiceException( "Should never be thrown!" );
    }


    @Override
    public boolean previous() throws PrismInterfaceServiceException {
        return relative( -1 );
    }


    @Override
    public void beforeFirst() throws PrismInterfaceServiceException {
        absolute( 0 );
    }


    @Override
    public void afterLast() {
        // Explanation: This is not an off by one error:
        // An index equal to the array size is one position after the last element.
        currentIndex = values.size();
        currentRow = null;
    }


    @Override
    public boolean first() {
        currentRow = null;
        currentIndex = INDEX_BEFORE_FIRST;
        if ( values.isEmpty() ) {
            return false;
        }
        currentIndex = 0;
        currentRow = values.get( currentIndex );
        return true;
    }


    @Override
    public boolean last() throws InterruptedException {
        currentRow = null;
        if ( resultFetcher.isLast() ) {
            currentIndex = values.size() - 1;
            currentRow = values.get( currentIndex );
            return true;
        }
        fetchAll();
        currentIndex = values.size() - 1;
        currentRow = values.get( currentIndex );
        return true;
    }


    @Override
    public boolean next() throws PrismInterfaceServiceException {
        try {
            considerPrefetch();
            syncFetch();
            currentIndex++;
            currentRow = values.get( currentIndex );
            if ( currentRow == null ) {
                return false;
            }
            return true;
        } catch ( InterruptedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Fetching mor rows from server failed.", e );
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


    private void syncFetch() throws InterruptedException {
        if ( fetcherThread == null ) {
            return;
        }
        // currently not at last element thus we don't have to wait on next frame
        if ( !(currentIndex == values.size() - 1) ) {
            return;
        }
        fetcherThread.join();
        fetcherThread = null;
        values.addAll( resultFetcher.getFetchedValues() );
    }


    @Override
    public ArrayList<TypedValue> current() {
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
        return currentIndex == INDEX_BEFORE_FIRST;
    }


    @Override
    public boolean isAfterLast() {
        return values.isEmpty() || currentIndex == values.size();
    }


    @Override
    public boolean isFirst() {
        return currentIndex == 0;
    }


    @Override
    public boolean isLast() {
        return currentIndex == values.size() - 1;
    }


    @Override
    public int getRow() {
        return indexToRow( currentIndex );
    }


    @Override
    public boolean hasCurrent() {
        return currentRow != null;
    }

}

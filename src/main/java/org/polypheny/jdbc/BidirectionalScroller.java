package org.polypheny.jdbc;

import static java.lang.Math.min;

import java.sql.SQLException;
import java.util.ArrayList;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class BidirectionalScroller implements BidirectionalScrollable<ArrayList<TypedValue>> {

    private static final int INDEX_BEFORE_FIRST = -1;
    private static final int PREFETCH_DEFAULT = 20;
    private ArrayList<ArrayList<TypedValue>> values;
    private ArrayList<TypedValue> currentRow;
    private ResultFetcher resultFetcher;
    private int prefetch_count;
    private Thread fetcherThread;
    int currentIndex;


    public BidirectionalScroller( Frame frame, ProtoInterfaceClient client, int statementId, int fetchSize ) {
        this.values = new ArrayList<>( TypedValueUtils.buildRows( frame.getRelationalFrame().getRowsList() ) );
        this.resultFetcher = new ResultFetcher( client, statementId, fetchSize );
        this.resultFetcher.setLast( frame.getIsLast() );
        this.currentIndex = INDEX_BEFORE_FIRST;
        this.prefetch_count = PREFETCH_DEFAULT;
    }

    public void setFetchSize(int fetchSize) {
        resultFetcher.setFetchSize( fetchSize );
        prefetch_count = min(PREFETCH_DEFAULT, fetchSize);
    }

    private int getFethSize() {
        return resultFetcher.getFetchSize();
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


    private void fetchAll() throws InterruptedException {
        while ( !resultFetcher.isLast() ) {
            fetcherThread = new Thread( resultFetcher );
            fetcherThread.start();
            fetcherThread.join();
        }
    }


    @Override
    public boolean absolute( int rowIndex ) throws SQLException {
        try {
            if ( rowToIndex( rowIndex ) == currentIndex ) {
                return true;
            }
            if ( rowIndex < 0 ) {
                fetchAll();
                currentIndex = values.size() + rowIndex;
                currentRow = values.get( currentIndex );
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
                return true;
            }
            // Explanation: This is not an off by one error:
            // An index equal to the array size is one position after the last element.
            currentIndex = values.size();
            currentRow = null;
            return false;
        } catch ( InterruptedException e ) {
            throw new SQLException( e );
        }
    }


    private int rowToIndex( int rowIndex ) {
        return rowIndex - 1;
    }


    private int indexToRow( int index ) {
        return index + 1;
    }


    @Override
    public boolean relative( int offset ) throws SQLException {
        try {
            if ( offset == 0 ) {
                return currentRow != null;
            }
            if ( currentIndex + offset < 0 ) {
                currentIndex = INDEX_BEFORE_FIRST;
                currentRow = null;
                return true;
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
                    return true;
                }
                // Explanation: This is not an off by one error:
                // An index equal to the array size is one position after the last element.
                currentIndex = values.size();
                currentRow = null;
                return false;
            }
        } catch ( InterruptedException e ) {
            throw new SQLException( e );
        }
        throw new ProtoInterfaceServiceException( "Should never be thrown!" );
    }


    @Override
    public boolean previous() throws SQLException {
        return relative( -1 );
    }


    @Override
    public boolean next() throws SQLException {
        return relative( +1 );
    }


    @Override
    public ArrayList<TypedValue> current() {
        return currentRow;
    }


    @Override
    public void close() {

    }


    @Override
    public boolean isBeforeFirst() {
        return currentIndex == INDEX_BEFORE_FIRST;
    }


    @Override
    public boolean isAfterLast() {
        return currentIndex == values.size();
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
}

package org.polypheny.jdbc;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.Frame.ResultCase;
import org.polypheny.db.protointerface.proto.Row;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ResultFetcher implements Runnable {

    private PrismInterfaceClient client;
    private int statementId;
    @Setter
    @Getter
    private PolyphenyResultSetProperties properties;
    private int fetchTimeout;
    private long totalFetched;
    @Setter
    @Getter
    private boolean isLast;
    @Getter
    private List<List<TypedValue>> fetchedValues;


    public ResultFetcher( PrismInterfaceClient client, int statementId, PolyphenyResultSetProperties properties, long totalFetched, int fetchTimeout ) {
        this.fetchTimeout = fetchTimeout;
        this.client = client;
        this.statementId = statementId;
        this.properties = properties;
        this.totalFetched = totalFetched;
        this.isLast = false;
    }


    @Override
    public void run() {
        long fetchEnd = totalFetched + properties.getStatementFetchSize();
        Frame nextFrame;
        try {
            nextFrame = client.fetchResult( statementId, properties.getFetchSize(), fetchTimeout );
        } catch ( PrismInterfaceServiceException e ) {
            throw new RuntimeException( e );
        }
        if ( nextFrame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new RuntimeException( new PrismInterfaceServiceException( "Illegal result type." ) );
        }
        List<Row> rows = nextFrame.getRelationalFrame().getRowsList();
        if ( properties.getLargeMaxRows() != 0 && fetchEnd > properties.getLargeMaxRows() ) {
            long rowEndIndex = properties.getLargeMaxRows() - totalFetched;
            if ( rowEndIndex > Integer.MAX_VALUE ) {
                throw new RuntimeException( "Should never be thrown" );
            }
            rows = rows.subList( 0, (int) rowEndIndex );
        }
        fetchedValues = TypedValueUtils.buildRows( rows );
        totalFetched = totalFetched + rows.size();
        isLast = nextFrame.getIsLast();
    }

}

package org.polypheny.jdbc;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.jdbctypes.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ResultFetcher implements Runnable {

    private ProtoInterfaceClient client;
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
    private List<ArrayList<TypedValue>> fetchedValues;


    public ResultFetcher( ProtoInterfaceClient client, int statementId, PolyphenyResultSetProperties properties, long totalFetched, int fetchTimeout ) {
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
            nextFrame = client.fetchResult( statementId, properties.getFetchSize(), fetchTimeout);
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( e );
        }
        if ( nextFrame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new RuntimeException(new ProtoInterfaceServiceException( "Illegal result type." ));
        }
        List<Row> rows = nextFrame.getRelationalFrame().getRowsList();
        if (properties.getLargeMaxRows() != 0 && fetchEnd > properties.getLargeMaxRows()) {
            long rowEndIndex = properties.getLargeMaxRows() - totalFetched;
            if (rowEndIndex > Integer.MAX_VALUE) {
                throw new RuntimeException("Should never be thrown");
            }
            rows = rows.subList(0, (int) rowEndIndex);
        }
        fetchedValues = TypedValueUtils.buildRows(rows);
        totalFetched = totalFetched + rows.size();
        isLast = nextFrame.getIsLast();

    }

}

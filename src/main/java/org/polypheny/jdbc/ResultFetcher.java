package org.polypheny.jdbc;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.properties.ResultSetProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ResultFetcher implements Runnable {

    private ProtoInterfaceClient client;
    private int statementId;
    @Setter
    @Getter
    private ResultSetProperties properties;
    private long offset;
    @Setter
    @Getter
    private boolean isLast;
    @Getter
    private List<ArrayList<TypedValue>> fetchedValues;


    public ResultFetcher( ProtoInterfaceClient client, int statementId, ResultSetProperties properties, long offset ) {
        this.client = client;
        this.statementId = statementId;
        this.properties = properties;
        this.offset = offset;
        this.isLast = false;
    }


    @Override
    public void run() {
        long fetchEnd = offset + properties.getFetchSize();
        Frame nextFrame = client.fetchResult( statementId, fetchEnd);
        if ( nextFrame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new ProtoInterfaceServiceException( "Illegal result type." );
        }
        List<Row> rows = nextFrame.getRelationalFrame().getRowsList();
        if (fetchEnd > properties.getLargeMaxRows()) {
            long rowEndIndex = properties.getLargeMaxRows() - offset;
            if (rowEndIndex > Integer.MAX_VALUE) {
                throw new RuntimeException("Should never be thrown");
            }
            rows = rows.subList(0, (int) rowEndIndex);
        }
        fetchedValues = TypedValueUtils.buildRows(rows);

        isLast = nextFrame.getIsLast();
        offset = nextFrame.getOffset();
    }

}

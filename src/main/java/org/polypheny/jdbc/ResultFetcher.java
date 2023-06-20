package org.polypheny.jdbc;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class ResultFetcher implements Runnable {

    private ProtoInterfaceClient client;
    private int statementId;
    @Setter
    @Getter
    private int fetchSize;
    private long offset;
    @Setter
    @Getter
    private boolean isLast;
    @Getter
    private List<ArrayList<TypedValue>> fetchedValues;


    public ResultFetcher( ProtoInterfaceClient client, int statementId, int fetchSize ) {
        this.client = client;
        this.statementId = statementId;
        this.fetchSize = fetchSize;
        this.offset = 0;
        this.isLast = false;
    }


    @Override
    public void run() {
        Frame nextFrame = client.fetchResult( statementId, offset + fetchSize );
        System.out.println( "Fetching offset: " + (offset + fetchSize) );
        if ( nextFrame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new ProtoInterfaceServiceException( "Illegal result type." );
        }
        fetchedValues = TypedValueUtils.buildRows( nextFrame.getRelationalFrame().getRowsList() );
        isLast = nextFrame.getIsLast();
        offset = nextFrame.getOffset();
    }

}

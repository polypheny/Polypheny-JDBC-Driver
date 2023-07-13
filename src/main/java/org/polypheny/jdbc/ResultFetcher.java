package org.polypheny.jdbc;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.properties.ResultSetProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
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


    public ResultFetcher( ProtoInterfaceClient client, int statementId, ResultSetProperties properties ) {
        this.client = client;
        this.statementId = statementId;
        this.properties = properties;
        this.offset = 0;
        this.isLast = false;
    }


    @Override
    public void run() {
        Frame nextFrame = client.fetchResult( statementId, offset + properties.getFetchSize() );
        System.out.println( "Fetching offset: " + (offset + properties.getFetchSize()) );
        if ( nextFrame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new ProtoInterfaceServiceException( "Illegal result type." );
        }
        fetchedValues = TypedValueUtils.buildRows( nextFrame.getRelationalFrame().getRowsList() );
        isLast = nextFrame.getIsLast();
        offset = nextFrame.getOffset();
    }

}

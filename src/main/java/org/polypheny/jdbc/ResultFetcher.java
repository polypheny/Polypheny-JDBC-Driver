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

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;
import org.polypheny.prism.Row;

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

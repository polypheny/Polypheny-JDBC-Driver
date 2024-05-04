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

package org.polypheny.jdbc.multimodel;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.Frame.ResultCase;
import org.polypheny.db.protointerface.proto.RelationalFrame;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.properties.PropertyUtils;

public class RelationalResult extends Result implements Iterable<PolyRow> {

    private final PolyStatement polyStatement;
    private final List<PolyRow> rows;
    private boolean isFullyFetched;


    public RelationalResult( Frame frame, PolyStatement polyStatement ) throws PrismInterfaceServiceException {
        super( ResultType.RELATIONAL );
        this.polyStatement = polyStatement;
        this.isFullyFetched = frame.getIsLast();
        this.rows = new ArrayList<>();
        addRows( frame.getRelationalFrame() );
    }


    private void addRows( RelationalFrame relationalFrame ) {
        relationalFrame.getRowsList().forEach( d -> rows.add( PolyRow.fromProto( d ) ) );
    }


    private void fetchMore() throws PrismInterfaceServiceException {
        int id = polyStatement.getStatementId();
        int timeout = getPolyphenyConnection().getTimeout();
        Frame frame = getProtoInterfaceClient().fetchResult( id, timeout, PropertyUtils.getDEFAULT_FETCH_SIZE() );
        if ( frame.getResultCase() != ResultCase.DOCUMENT_FRAME ) {
            throw new PrismInterfaceServiceException(
                    PrismInterfaceErrors.RESULT_TYPE_INVALID,
                    "Statement returned a result of illegal type " + frame.getResultCase()
            );
        }
        isFullyFetched = frame.getIsLast();
        addRows( frame.getRelationalFrame() );
    }


    private PolyConnection getPolyphenyConnection() {
        return polyStatement.getConnection();
    }


    private PrismInterfaceClient getProtoInterfaceClient() {
        return getPolyphenyConnection().getProtoInterfaceClient();
    }


    @Override
    public Iterator<PolyRow> iterator() {
        return new RelationalResult.RowIterator();
    }


    class RowIterator implements Iterator<PolyRow> {

        int index = -1;


        @Override
        public boolean hasNext() {
            if ( index + 1 >= rows.size() ) {
                if ( isFullyFetched ) {
                    return false;
                }
                try {
                    fetchMore();
                } catch ( PrismInterfaceServiceException e ) {
                    throw new RuntimeException( e );
                }
            }
            return index + 1 < rows.size();
        }


        @Override
        public PolyRow next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException( "There are no more documents" );
            }
            return rows.get( ++index );
        }

    }

}

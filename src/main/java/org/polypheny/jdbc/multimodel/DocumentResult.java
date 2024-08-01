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
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.types.PolyDocument;
import org.polypheny.prism.DocumentFrame;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;

public class DocumentResult extends Result implements Iterable<PolyDocument> {

    private final PolyStatement polyStatement;
    private final List<PolyDocument> documents;
    private boolean isFullyFetched;


    public DocumentResult( Frame frame, PolyStatement polyStatement ) {
        super( ResultType.DOCUMENT );
        this.polyStatement = polyStatement;
        this.isFullyFetched = frame.getIsLast();
        this.documents = new ArrayList<>();
        addDocuments( frame.getDocumentFrame() );
    }


    private void addDocuments( DocumentFrame documentFrame ) {
        documentFrame.getDocumentsList().forEach( d -> documents.add( new PolyDocument( d, polyStatement.getConnection() ) ) );
    }


    private void fetchMore() throws PrismInterfaceServiceException {
        int id = polyStatement.getStatementId();
        int timeout = getPolyphenyConnection().getTimeout();
        Frame frame = getPrismInterfaceClient().fetchResult( id, timeout, PropertyUtils.getDEFAULT_FETCH_SIZE() );
        if ( frame.getResultCase() != ResultCase.DOCUMENT_FRAME ) {
            throw new PrismInterfaceServiceException(
                    PrismInterfaceErrors.RESULT_TYPE_INVALID,
                    "Statement returned a result of illegal type " + frame.getResultCase()
            );
        }
        isFullyFetched = frame.getIsLast();
        addDocuments( frame.getDocumentFrame() );
    }


    private PolyConnection getPolyphenyConnection() {
        return polyStatement.getConnection();
    }


    private PrismInterfaceClient getPrismInterfaceClient() {
        return getPolyphenyConnection().getPrismInterfaceClient();
    }


    @Override
    public Iterator<PolyDocument> iterator() {
        return new DocumentIterator();
    }


    class DocumentIterator implements Iterator<PolyDocument> {

        int index = -1;


        @Override
        public boolean hasNext() {
            if ( index + 1 >= documents.size() ) {
                if ( isFullyFetched ) {
                    return false;
                }
                try {
                    fetchMore();
                } catch ( PrismInterfaceServiceException e ) {
                    throw new RuntimeException( e );
                }
            }
            return index + 1 < documents.size();
        }


        @Override
        public PolyDocument next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException( "There are no more documents" );
            }
            return documents.get( ++index );
        }

    }

}

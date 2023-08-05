/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.util.NoSuchElementException;
import org.polypheny.jdbc.PolyphenyConnection;
import org.polypheny.jdbc.ProtoInterfaceClient;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.DocumentFrame;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.proto.ProtoDocument;

public class DocumentResult implements Iterable<Document> {

    ProtoStatement protoStatement;
    ArrayList<Document> documents;
    boolean isFullyFetched;


    public DocumentResult( DocumentFrame documentFrame, ProtoStatement protoStatement ) throws ProtoInterfaceServiceException {
        this.protoStatement = protoStatement;
        this.isFullyFetched = false;
        this.documents = new ArrayList<>();
        addDocuments( documentFrame );
    }


    private void addDocuments( DocumentFrame documentFrame ) throws ProtoInterfaceServiceException {
        for ( ProtoDocument protoDocument : documentFrame.getDocumentsList() ) {
            documents.add( new Document( protoDocument ) );
        }
    }


    private void fetchMore() throws ProtoInterfaceServiceException {
        int id = protoStatement.getStatementId();
        int timeout = getPolyphenyConnection().getTimeout();
        Frame frame = getProtoInterfaceClient().fetchResult( id, timeout );
        if ( frame.getResultCase() != ResultCase.DOCUMENT_FRAME ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.RESULT_TYPE_INVALID,
                    "Statement returned a result of illegal type "
                            + frame.getResultCase()
            );
        }
        isFullyFetched = frame.getIsLast();
        addDocuments( frame.getDocumentFrame() );
    }


    private PolyphenyConnection getPolyphenyConnection() {
        return protoStatement.getConnection();
    }


    private ProtoInterfaceClient getProtoInterfaceClient() {
        return getPolyphenyConnection().getProtoInterfaceClient();
    }


    @Override
    public Iterator<Document> iterator() {
        return new DocumentIterator();
    }


    class DocumentIterator implements Iterator<Document> {

        int index = -1;
        Document current = null;


        @Override
        public boolean hasNext() {
            if ( ++index >= documents.size() ) {
                if ( isFullyFetched ) {
                    return false;
                }
                try {
                    fetchMore();
                } catch ( ProtoInterfaceServiceException e ) {
                    throw new RuntimeException( e );
                }
            }
            try {
                current = documents.get( index );
                return true;
            } catch ( IndexOutOfBoundsException ignored ) {
            }
            return false;
        }


        @Override
        public Document next() {
            if ( current == null ) {
                throw new NoSuchElementException( "There are no more documents" );
            }
            return current;
        }

    }

}

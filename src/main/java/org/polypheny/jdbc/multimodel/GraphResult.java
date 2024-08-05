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
import org.polypheny.jdbc.types.PolyEdge;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.types.PolyNode;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;
import org.polypheny.prism.GraphFrame;

public class GraphResult extends Result implements Iterable<PolyGraphElement> { // implements iterable over some graph representation

    private final PolyStatement polyStatement;
    private boolean isFullyFetched;
    private final List<PolyGraphElement> elements;


    public GraphResult( Frame frame, PolyStatement polyStatement ) {
        super( ResultType.GRAPH );
        this.polyStatement = polyStatement;
        this.isFullyFetched = frame.getIsLast();
        this.elements = new ArrayList<>();
        addGraphElements( frame.getGraphFrame() );
    }


    private void addGraphElements( GraphFrame graphFrame ) {
        graphFrame.getElementList().forEach( e -> {
                    switch ( e.getElementCase() ) {
                        case NODE:
                            elements.add( new PolyNode( e.getNode(), polyStatement.getConnection() ) );
                            break;
                        case EDGE:
                            elements.add( new PolyEdge( e.getEdge(), polyStatement.getConnection() ) );
                            break;
                    }
                }
        );
    }


    private void fetchMore() throws PrismInterfaceServiceException {
        int id = polyStatement.getStatementId();
        int timeout = getPolyphenyConnection().getTimeout();
        Frame frame = getPrismInterfaceClient().fetchResult( id, timeout, PropertyUtils.getDEFAULT_FETCH_SIZE() );
        if ( frame.getResultCase() != ResultCase.GRAPH_FRAME ) {
            throw new PrismInterfaceServiceException(
                    PrismInterfaceErrors.RESULT_TYPE_INVALID,
                    "Statement returned a result of illegal type " + frame.getResultCase()
            );
        }
        isFullyFetched = frame.getIsLast();
        addGraphElements( frame.getGraphFrame() );
    }


    private PolyConnection getPolyphenyConnection() {
        return polyStatement.getConnection();
    }


    private PrismInterfaceClient getPrismInterfaceClient() {
        return getPolyphenyConnection().getPrismInterfaceClient();
    }


    @Override
    public Iterator<PolyGraphElement> iterator() {
        return new GraphElementIterator();
    }


    class GraphElementIterator implements Iterator<PolyGraphElement> {

        int index = -1;


        @Override
        public boolean hasNext() {
            if ( index + 1 >= elements.size() ) {
                if ( isFullyFetched ) {
                    return false;
                }
                try {
                    fetchMore();
                } catch ( PrismInterfaceServiceException e ) {
                    throw new RuntimeException( e );
                }
            }
            return index + 1 < elements.size();
        }


        @Override
        public PolyGraphElement next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException( "There are no more graph elements" );
            }
            return elements.get( ++index );
        }

    }


}

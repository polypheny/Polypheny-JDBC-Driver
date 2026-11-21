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
import java.util.function.Function;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Frame.ResultCase;

abstract class PagedResult<T> extends Result implements Iterable<T> {

    private final PolyStatement statement;
    private final long execution;
    private final int fetchSize;
    private final ResultCase frameType;
    private final Function<Frame, List<T>> readFrame;
    private final List<T> elements;
    private boolean last;


    PagedResult( ResultType resultType, Frame firstFrame, PolyStatement statement, Function<Frame, List<T>> readFrame ) {
        super( resultType );
        this.statement = statement;
        this.execution = statement.getExecution();
        this.fetchSize = statement.getFetchSize();
        this.frameType = firstFrame.getResultCase();
        this.readFrame = readFrame;
        this.elements = new ArrayList<>( readFrame.apply( firstFrame ) );
        this.last = firstFrame.getIsLast();
    }


    private synchronized boolean hasElement( int index ) {
        while ( index >= elements.size() && !last ) {
            try {
                Frame frame = statement.fetchFrame( execution, fetchSize, frameType );
                elements.addAll( readFrame.apply( frame ) );
                last = frame.getIsLast();
            } catch ( PrismInterfaceServiceException e ) {
                throw new IllegalStateException( "Could not fetch the next result page.", e );
            }
        }
        return index < elements.size();
    }


    @Override
    public Iterator<T> iterator() {
        return new Iterator<T>() {
            private int index;


            @Override
            public boolean hasNext() {
                return hasElement( index );
            }


            @Override
            public T next() {
                synchronized ( PagedResult.this ) {
                    if ( !hasNext() ) {
                        throw new NoSuchElementException();
                    }
                    return elements.get( index++ );
                }
            }
        };
    }

}

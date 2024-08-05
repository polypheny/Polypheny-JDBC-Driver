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

package org.polypheny.jdbc.streaming;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.multimodel.PolyStatement;

public class StreamingIndex {

    private final HashSet<PrismOutputStream> streams;
    private int statementId = PolyStatement.NO_STATEMENT_ID;
    private final AtomicLong streamIdGenerator = new AtomicLong();
    private final PrismInterfaceClient prismInterfaceClient;


    public StreamingIndex( PrismInterfaceClient prismInterfaceClient ) {
        this.streams = new HashSet<>();
        this.prismInterfaceClient = prismInterfaceClient;
    }


    public long register( PrismOutputStream stream ) {
        streams.add( stream );
        long streamId = streamIdGenerator.getAndIncrement();
        stream.buildAndRun(statementId, streamId, prismInterfaceClient );
        return streamId;
    }


    public void update( int statementId ) {
        this.statementId = statementId;
    }

}

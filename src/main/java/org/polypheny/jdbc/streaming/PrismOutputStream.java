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

import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.multimodel.PolyStatement;

public abstract class PrismOutputStream extends Thread {

    private static final long NO_STREAM_ID = -1;
    protected static final int STREAMING_TIMEOUT = 100000;

    protected int statementId = PolyStatement.NO_STATEMENT_ID;
    protected long streamId = NO_STREAM_ID;


    protected void setStatementId( int statementId ) {
        if ( statementId != PolyStatement.NO_STATEMENT_ID ) {
            throw new IllegalStateException( "Statement id can only be set once." );
        }
    }


    protected void setStreamId( long streamId ) {
        if ( streamId != NO_STREAM_ID ) {
            throw new IllegalStateException( "Stream id can only be set once." );
        }
        this.streamId = streamId;
    }


    public abstract void buildAndRun( int statementId, long streamId, PrismInterfaceClient prismInterfaceClient );

}

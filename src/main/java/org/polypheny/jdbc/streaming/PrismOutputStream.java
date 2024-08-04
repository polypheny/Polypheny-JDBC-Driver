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

import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceClient;

public abstract class PrismOutputStream extends Thread {

    private static final long NO_STREAM_ID = -1;
    protected static final int STREAMING_TIMEOUT = 100000;

    protected long streamId = NO_STREAM_ID;


    protected void setStreamId( long streamId ) {
        if ( streamId != NO_STREAM_ID ) {
            throw new IllegalStateException();
        }
        this.streamId = streamId;
    }

    public abstract void buildAndRun(long statementId, PrismInterfaceClient prismInterfaceClient );
}

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
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.StreamAcknowledgement;

public class BinaryPrismOutputStream extends PrismOutputStream {

    private final byte[] data;
    private PrismInterfaceClient client;


    public BinaryPrismOutputStream( byte[] data) {
        this.data = data;
    }

    public void buildAndRun(long streamId, PrismInterfaceClient prismInterfaceClient ) {
        setStreamId( streamId );
        this.client = prismInterfaceClient;
    }

    @Override
    public void run() {
        if ( client == null ) {
            throw new IllegalStateException( "PrismInterfaceClient not set" );
        }
        int size = data.length;
        int offset = 0;

        while ( offset < size ) {
            int frameSize = Math.min( TypedValue.STREAMING_THRESHOLD, size - offset );
            byte[] frameData = new byte[frameSize];
            System.arraycopy( data, offset, frameData, 0, frameSize );
            boolean isLast = (offset + frameSize) >= size;
            try {
                StreamAcknowledgement ack = client.streamBinary( frameData, isLast, streamId, STREAMING_TIMEOUT );
                if ( ack.getCloseStream() ) {
                    return;
                }
            } catch ( PrismInterfaceServiceException e ) {
                throw new RuntimeException( "Error streaming binary data", e );
            }
            offset += frameSize;
        }
    }

}

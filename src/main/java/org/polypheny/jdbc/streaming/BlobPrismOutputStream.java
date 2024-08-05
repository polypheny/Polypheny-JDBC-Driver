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

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.SQLException;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.StreamAcknowledgement;

public class BlobPrismOutputStream extends PrismOutputStream {

    private final Blob blob;
    private PrismInterfaceClient client;



    public BlobPrismOutputStream( Blob blob ) {
        this.blob = blob;
        setName( "BinaryPrismOutputStream" );
    }


    public void buildAndRun(int statementId, long streamId, PrismInterfaceClient prismInterfaceClient ) {
        setStatementId( statementId );
        setStreamId( streamId );
        this.client = prismInterfaceClient;
        start();
    }


    @Override
    public void run() {
        if ( client == null ) {
            throw new IllegalStateException( "PrismInterfaceClient not set" );
        }
        long size;
        long offset = 0;

        try ( InputStream inputStream = blob.getBinaryStream() ) {
            size = blob.length();
            byte[] buffer = new byte[TypedValue.STREAMING_THRESHOLD];

            while ( offset < size ) {
                int bytesRead = inputStream.read( buffer, 0, TypedValue.STREAMING_THRESHOLD );
                if ( bytesRead == -1 ) {
                    break;
                }
                boolean isLast = (offset + bytesRead) >= size;
                byte[] frameData = new byte[bytesRead];
                System.arraycopy( buffer, 0, frameData, 0, bytesRead );
                StreamAcknowledgement ack = client.streamBinary( frameData, isLast, statementId, streamId, STREAMING_TIMEOUT );
                if ( ack.getCloseStream() ) {
                    return;
                }
                offset += bytesRead;
            }
        } catch ( SQLException | IOException e ) {
            throw new RuntimeException( "Error streaming binary data", e );
        }
    }

}

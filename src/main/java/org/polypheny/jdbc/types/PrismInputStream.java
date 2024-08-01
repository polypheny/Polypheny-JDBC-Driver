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

package org.polypheny.jdbc.types;

import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.StreamFrame;

public class PrismInputStream extends InputStream {

    private static final long NO_MARK = -1;
    private static final long NO_LIMIT = -1;
    private static final int BUFFER_SIZE = 1000;

    private final PolyConnection connection;
    private final int statementId;
    private final long streamId;
    private final long limit;

    @Getter
    private final boolean isForwardOnly;

    private long currentPosition = 0;
    private long markPosition = NO_MARK;
    private int markReadLimit = 0;

    private byte[] buffer;
    private long bufferStartPosition;

    private boolean isLast = false;
    private boolean isClosed = false;


    public PrismInputStream( int statementId, long streamId, boolean isForwardOnly, PolyConnection connection ) {
        this.connection = connection;
        this.statementId = statementId;
        this.streamId = streamId;
        this.isForwardOnly = isForwardOnly;
        this.buffer = new byte[BUFFER_SIZE];
        this.bufferStartPosition = 0;
        this.limit = NO_LIMIT;
    }

    public PrismInputStream(PrismInputStream other, long limit, long startPosition) {
        this.currentPosition = startPosition;
        this.connection = other.connection;
        this.statementId = other.statementId;
        this.streamId = other.streamId;
        this.isForwardOnly = other.isForwardOnly;
        this.buffer = new byte[BUFFER_SIZE];
        this.bufferStartPosition = other.bufferStartPosition;
        this.limit = limit;
    }


    @Override
    public int available() throws IOException {
        long available = bufferStartPosition + buffer.length - currentPosition;
        if ( limit != NO_LIMIT && limit < bufferStartPosition + buffer.length ) {
            available = limit - currentPosition;
        }
        if ( available > Integer.MAX_VALUE ) {
            return Integer.MAX_VALUE;
        }
        return Math.toIntExact( available );
    }


    @Override
    public void close() throws IOException {
        // No need to communicate with the server as all streams are closed on statement closure.
        isClosed = true;
    }


    @Override
    public void mark( int readlimit ) {
        markPosition = currentPosition;
        markReadLimit = readlimit;
    }


    @Override
    public void reset() throws IOException {
        if ( isForwardOnly ) {
            throw new IOException( "This stream does not support mark and reset." );
        }
        if ( markPosition == NO_MARK ) {
            throw new IOException( "No mark set. Nothing to reset." );
        }
        if ( currentPosition - markPosition > markReadLimit ) {
            throw new IOException( "Current position exceeds read limit set on mark. Nothing to reset." );
        }
    }


    @Override
    public boolean markSupported() {
        return !isForwardOnly;
    }


    private int getBufferReadPosition() {
        return Math.toIntExact( currentPosition - bufferStartPosition );
    }


    @Override
    public int read() throws IOException {
        try {
            fetchIfEmpty();
        } catch ( IOException e ) {
            return -1;
        }
        int bufferReadPosition = getBufferReadPosition();
        currentPosition++;
        return buffer[bufferReadPosition] & 0xFF;
    }


    private void fetchIfEmpty() throws IOException {
        if ( available() <= 0 ) {
            if ( isLast ) {
                throw new IOException( "No more data." );
            }
            fetchNextBytes();
        }
    }


    private boolean hasMoreData() throws IOException {
        return !isLast || available() > 0;
    }


    private void fetchNextBytes() throws IOException {
        long fetchPosition = bufferStartPosition + buffer.length;
        int timeout = connection.getTimeout();
        StreamFrame frame;
        try {
            frame = connection.getPrismInterfaceClient().fetchStream( this.statementId, this.streamId, fetchPosition, BUFFER_SIZE, timeout );
        } catch ( PrismInterfaceServiceException e ) {
            throw new IOException( e );
        }
        this.isLast = frame.getIsLast();
        this.bufferStartPosition = fetchPosition;
        this.buffer = frame.getData().toByteArray();
    }


    @Override
    public int read( byte[] b ) throws IOException {
        try {
            fetchIfEmpty();
        } catch ( IOException e ) {
            return -1;
        }
        int bytesCopied = 0;
        while ( bytesCopied < b.length && hasMoreData() ) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min( available(), b.length - bytesCopied );
            System.arraycopy( this.buffer, bufferReadPosition, b, bytesCopied, bytesToCopy );
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytesCopied;
    }


    private void reposition( long pos, int len ) throws IOException {
        if ( pos < bufferStartPosition && isForwardOnly ) {
            throw new IOException( "Can't access already returned section of a forward only stream." );
        }

        if ( pos < currentPosition ) {
            if ( pos + len < markPosition ) {
                markPosition = NO_MARK;
            }
            currentPosition = pos;
            if ( !isForwardOnly ) {
                bufferStartPosition = pos;
                isLast = false;
                fetchNextBytes();
            }
            return;
        }
        skip( pos - currentPosition );
    }


    public byte[] getBytes( long pos, int len ) throws IOException {
        byte[] bytes = new byte[len];
        int bytesCopied = 0;

        reposition( pos, len );

        while ( bytesCopied < len && hasMoreData() ) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min( available(), len - bytesCopied );
            System.arraycopy( this.buffer, bufferReadPosition, bytes, bytesCopied, bytesToCopy );
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytes;
    }


    @Override
    public int read( byte[] b, int off, int len ) throws IOException {
        int bytesCopied = 0;
        while ( bytesCopied < len && hasMoreData() ) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min( available(), len - bytesCopied );
            System.arraycopy( this.buffer, bufferReadPosition, b, off + bytesCopied, bytesToCopy );
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytesCopied;
    }


    @Override
    public long skip( long n ) throws IOException {
        long bytesSkipped = 0;
        while ( bytesSkipped < n && hasMoreData() ) {
            long skipped = Math.min( n - bytesSkipped, available() );
            bytesSkipped += skipped;
            currentPosition += skipped;
            fetchIfEmpty();
        }
        return bytesSkipped;
    }

}

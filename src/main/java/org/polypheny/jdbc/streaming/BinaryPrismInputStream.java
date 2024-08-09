package org.polypheny.jdbc.streaming;

import java.io.IOException;
import java.io.InputStream;
import lombok.Getter;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamFrame.DataCase;

public class BinaryPrismInputStream extends InputStream {

    private static final long NO_MARK = -1;
    private static final long NO_LIMIT = -1;
    private static final int BUFFER_SIZE = 10000;

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

    public BinaryPrismInputStream(int statementId, long streamId, boolean isForwardOnly, PolyConnection connection) {
        this.connection = connection;
        this.statementId = statementId;
        this.streamId = streamId;
        this.isForwardOnly = isForwardOnly;
        this.buffer = new byte[0];
        this.bufferStartPosition = 0;
        this.limit = NO_LIMIT;
    }

    public BinaryPrismInputStream(BinaryPrismInputStream other, long limit, long startPosition) {
        this.currentPosition = startPosition;
        this.connection = other.connection;
        this.statementId = other.statementId;
        this.streamId = other.streamId;
        this.isForwardOnly = other.isForwardOnly;
        this.buffer = other.buffer.clone();
        this.bufferStartPosition = other.bufferStartPosition;
        this.limit = limit;
    }

    @Override
    public int available() throws IOException {
        long available = bufferStartPosition + buffer.length - currentPosition;
        if (limit != NO_LIMIT && limit < bufferStartPosition + buffer.length) {
            available = limit - currentPosition;
        }
        if (available > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return Math.toIntExact(available);
    }

    @Override
    public void close() throws IOException {
        // No need to communicate with the server as all streams are closed on statement closure.
        isClosed = true;
    }

    @Override
    public void mark(int readlimit) {
        if (isForwardOnly) {
            return;  // Forward-only streams do not support mark
        }
        markPosition = currentPosition;
        markReadLimit = readlimit;
    }

    @Override
    public void reset() throws IOException {
        if (isForwardOnly) {
            throw new IOException("This stream does not support mark and reset.");
        }
        if (markPosition == NO_MARK) {
            throw new IOException("No mark set. Nothing to reset.");
        }
        if (currentPosition - markPosition > markReadLimit) {
            throw new IOException("Current position exceeds read limit set on mark. Nothing to reset.");
        }
        currentPosition = markPosition;
        bufferStartPosition = markPosition;
        fetchNextBytes();  // Refill the buffer from the marked position
    }

    @Override
    public boolean markSupported() {
        return !isForwardOnly;
    }

    private int getBufferReadPosition() {
        return Math.toIntExact(currentPosition - bufferStartPosition);
    }

    @Override
    public int read() throws IOException {
        fetchIfEmpty();
        if (!hasMoreData()) {
            return -1;
        }
        int bufferReadPosition = getBufferReadPosition();
        currentPosition++;
        return buffer[bufferReadPosition] & 0xFF;
    }

    private void fetchIfEmpty() throws IOException {
        if (available() <= 0) {
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
            frame = connection.getPrismInterfaceClient().fetchStream(this.statementId, this.streamId, fetchPosition, BUFFER_SIZE, timeout);
        } catch (PrismInterfaceServiceException e) {
            throw new IOException(e);
        }
        this.isLast = frame.getIsLast();
        this.bufferStartPosition = fetchPosition;
        if (frame.getDataCase() != DataCase.BINARY) {
            throw new RuntimeException("Stream type must be binary.");
        }
        this.buffer = frame.getBinary().toByteArray();
    }

    @Override
    public int read(byte[] b) throws IOException {
        fetchIfEmpty();
        if (!hasMoreData()) {
            return 1;
        }
        int bytesCopied = 0;
        while (bytesCopied < b.length && hasMoreData()) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min(available(), b.length - bytesCopied);
            System.arraycopy(this.buffer, bufferReadPosition, b, bytesCopied, bytesToCopy);
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytesCopied;
    }

    private void reposition(long pos, int len) throws IOException {
        if (pos < bufferStartPosition && isForwardOnly) {
            throw new IOException("Can't access already returned section of a forward-only stream.");
        }

        if (pos < currentPosition) {
            if (pos + len < markPosition) {
                markPosition = NO_MARK;
            }
            currentPosition = pos;
            if (!isForwardOnly) {
                bufferStartPosition = pos;
                isLast = false;
                fetchNextBytes();
            }
            return;
        }
        skip(pos - currentPosition);
    }

    public byte[] getBytes(long pos, int len) throws IOException {
        byte[] bytes = new byte[len];
        int bytesCopied = 0;

        reposition(pos, len);

        while (bytesCopied < len && hasMoreData()) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min(available(), len - bytesCopied);
            System.arraycopy(this.buffer, bufferReadPosition, bytes, bytesCopied, bytesToCopy);
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytes;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        fetchIfEmpty();
        if (!hasMoreData()) {
            return -1;
        }
        int bytesCopied = 0;
        while (bytesCopied < len && hasMoreData()) {
            int bufferReadPosition = getBufferReadPosition();
            int bytesToCopy = Math.min(available(), len - bytesCopied);
            System.arraycopy(this.buffer, bufferReadPosition, b, off + bytesCopied, bytesToCopy);
            currentPosition += bytesToCopy;
            bytesCopied += bytesToCopy;
            fetchIfEmpty();
        }
        return bytesCopied;
    }

    @Override
    public long skip(long n) throws IOException {
        long bytesSkipped = 0;
        while (bytesSkipped < n && hasMoreData()) {
            long skipped = Math.min(n - bytesSkipped, available());
            bytesSkipped += skipped;
            currentPosition += skipped;
            fetchIfEmpty();
        }
        return bytesSkipped;
    }
}

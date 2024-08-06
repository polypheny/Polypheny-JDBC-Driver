package org.polypheny.jdbc.streaming;

import java.io.IOException;
import java.io.Reader;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamFrame.DataCase;

public class StringPrismInputStream extends Reader {

    private static final long NO_MARK = -1;
    private static final long NO_LIMIT = -1;
    private static final int BUFFER_SIZE = 10000;

    private final PolyConnection connection;
    private final int statementId;
    private final long streamId;
    private final boolean isForwardOnly;

    private long currentPosition = 0;
    private long markPosition = NO_MARK;
    private int markReadLimit = 0;

    private String buffer = "";
    private long bufferStartPosition = 0;

    private boolean isLast = false;
    private boolean isClosed = false;

    public StringPrismInputStream(int statementId, long streamId, boolean isForwardOnly, PolyConnection connection) {
        this.statementId = statementId;
        this.streamId = streamId;
        this.isForwardOnly = isForwardOnly;
        this.connection = connection;
    }

    @Override
    public int read(char[] chars, int off, int len) throws IOException {
        fetchIfEmpty();
        if (!hasMoreData()) {
            return -1; // End of stream
        }

        int charsCopied = 0;
        while (charsCopied < len && hasMoreData()) {
            int bufferReadPosition = getBufferReadPosition();
            int charsToCopy = Math.min(available(), len - charsCopied);
            buffer.getChars(bufferReadPosition, bufferReadPosition + charsToCopy, chars, off + charsCopied);
            currentPosition += charsToCopy;
            charsCopied += charsToCopy;
            fetchIfEmpty();
        }
        return charsCopied;
    }

    @Override
    public void close() throws IOException {
        isClosed = true;
    }

    @Override
    public void mark(int readAheadLimit) throws IOException {
        if (isForwardOnly) {
            throw new IOException("Mark not supported in forward-only streams.");
        }
        markPosition = currentPosition;
        markReadLimit = readAheadLimit;
    }

    @Override
    public void reset() throws IOException {
        if (isForwardOnly) {
            throw new IOException("Reset not supported in forward-only streams.");
        }
        if (markPosition == NO_MARK) {
            throw new IOException("Mark not set.");
        }
        if (currentPosition - markPosition > markReadLimit) {
            throw new IOException("Read limit exceeded.");
        }
        currentPosition = markPosition;
    }

    @Override
    public boolean markSupported() {
        return !isForwardOnly;
    }

    private void fetchIfEmpty() throws IOException {
        if (available() <= 0) {
            fetchNextChars();
        }
    }

    private boolean hasMoreData() throws IOException {
        return !isLast || available() > 0;
    }

    private void fetchNextChars() throws IOException {
        long fetchPosition = bufferStartPosition + buffer.length();
        int timeout = connection.getTimeout();
        StreamFrame frame;
        try {
            frame = connection.getPrismInterfaceClient().fetchStream(statementId, streamId, fetchPosition, BUFFER_SIZE, timeout);
        } catch (PrismInterfaceServiceException e) {
            throw new IOException(e);
        }
        this.isLast = frame.getIsLast();
        this.bufferStartPosition = fetchPosition;
        if (frame.getDataCase() != DataCase.STRING) {
            throw new RuntimeException("Stream type must be string.");
        }
        this.buffer = frame.getString();
    }

    private int available() {
        long available = bufferStartPosition + buffer.length() - currentPosition;
        return (int) Math.min(available, Integer.MAX_VALUE);
    }

    private int getBufferReadPosition() {
        return (int) (currentPosition - bufferStartPosition);
    }
}

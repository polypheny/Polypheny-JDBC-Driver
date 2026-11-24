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

package org.polypheny.jdbc.transport;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;

public class PlainTransport implements Transport {

    private final static String VERSION = "plain-v1@polypheny.com";

    protected final SocketChannel con;
    private final Lock writeLock = new ReentrantLock();


    public PlainTransport( String host, int port ) throws IOException {
        con = SocketChannel.open( new InetSocketAddress( host, port ) );
        con.setOption( StandardSocketOptions.TCP_NODELAY, true );
        exchangeVersion();
    }


    private void exchangeVersion() throws IOException {
        ByteBuffer length = ByteBuffer.allocate( 1 );
        readEntireBuffer( length );
        byte len = length.get();
        if ( len <= 0 ) {
            throw new IOException( "Invalid version length" );
        }
        ByteBuffer response = ByteBuffer.allocate( 1 + len ); // Leading size
        response.put( len );
        readEntireBuffer( response );
        byte[] remoteVersion = new byte[len - 1]; // trailing newline
        response.position( 1 );
        response.get( remoteVersion );
        if ( !Arrays.equals( VERSION.getBytes( StandardCharsets.US_ASCII ), remoteVersion ) ) {
            String s = StandardCharsets.US_ASCII.decode( ByteBuffer.wrap( remoteVersion ) ).toString();
            if ( s.matches( "\\A[a-z0-9@.-]+\\z" ) ) {
                throw new IOException( "Unsupported version: '" + s + "' expected '" + VERSION + "'" );
            } else {
                throw new IOException( "Unsupported version" );
            }
        }
        if ( response.get() != (byte) 0x0a ) {
            throw new IOException( "Invalid version message" );
        }
        response.rewind();
        writeEntireBuffer( response );
    }


    protected void writeEntireBuffer( ByteBuffer bb ) throws IOException {
        writeLock.lock();
        try {
            while ( bb.remaining() > 0 ) {
                int i = con.write( bb );
                if ( i == -1 ) {
                    throw new EOFException();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }


    @Override
    public void sendMessage( byte[] message ) throws IOException {
        writeEntireBuffer( frameMessage( message ) );
    }


    @Override
    public boolean trySendMessage( byte[] message, long deadlineNanos, BooleanSupplier beginWrite ) throws IOException {
        ByteBuffer frame = frameMessage( message );
        long remaining = deadlineNanos - System.nanoTime();
        if ( remaining <= 0 ) {
            return false;
        }
        try {
            if ( !writeLock.tryLock( remaining, TimeUnit.NANOSECONDS ) ) {
                return false;
            }
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            ClosedByInterruptException interrupted = new ClosedByInterruptException();
            interrupted.initCause( e );
            throw interrupted;
        }
        try {
            if ( !beginWrite.getAsBoolean() ) {
                return false;
            }
            writeEntireBuffer( frame );
            return true;
        } finally {
            writeLock.unlock();
        }
    }


    @Override
    public <E extends Exception> boolean withWriteReservation( int timeoutMillis, WriteOperation<E> operation ) throws E, InterruptedException {
        if ( timeoutMillis == 0 ) {
            writeLock.lockInterruptibly();
        } else if ( !writeLock.tryLock( timeoutMillis, TimeUnit.MILLISECONDS ) ) {
            return false;
        }
        try {
            operation.run();
            return true;
        } finally {
            writeLock.unlock();
        }
    }


    private static ByteBuffer frameMessage( byte[] message ) {
        ByteBuffer frame = ByteBuffer.allocate( 8 + message.length ).order( ByteOrder.LITTLE_ENDIAN );
        frame.putLong( message.length );
        frame.put( message );
        frame.rewind();
        return frame;
    }


    protected void readEntireBuffer( ByteBuffer bb ) throws IOException {
        while ( bb.remaining() > 0 ) {
            int i = con.read( bb );
            if ( i == -1 ) {
                throw new EOFException();
            }
        }
        bb.rewind();
    }


    @Override
    public byte[] receiveMessage() throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 8 );
        readEntireBuffer( bb );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        if ( length == 0 ) {
            throw new IOException( "Invalid message length" );
        }
        bb = ByteBuffer.allocate( (int) length );
        readEntireBuffer( bb );
        return bb.array();
    }


    @Override
    public void close() {
        try {
            con.close();
        } catch ( IOException ignore ) {
            // ignore
        }
    }

}

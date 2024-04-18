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
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PlainTransport implements Transport {

    private final static byte[] VERSION = "plain-v1@polypheny.com".getBytes( StandardCharsets.US_ASCII );

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
        byte[] remoteTransport = new byte[len - 1]; // trailing newline
        response.position( 1 );
        response.get( remoteTransport );
        if ( !Arrays.equals( VERSION, remoteTransport ) ) {
            String s = StandardCharsets.US_ASCII.decode( response ).toString();
            if ( s.matches( "\\A[a-z0-9@.-]+\\z" ) ) {
                throw new IOException( "Unsupported version: " + s );
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
        ByteBuffer bb = ByteBuffer.allocate( 8 + message.length );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( message.length );
        bb.put( message );
        bb.rewind();
        writeEntireBuffer( bb );
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

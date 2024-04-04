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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PlainTransport implements Transport {

    protected final SocketChannel con;
    private final Lock writeLock = new ReentrantLock();


    public PlainTransport( String host, int port ) throws IOException {
        con = SocketChannel.open( new InetSocketAddress( host, port ) );
        con.setOption( StandardSocketOptions.TCP_NODELAY, true );
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

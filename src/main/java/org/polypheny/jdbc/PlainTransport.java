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

package org.polypheny.jdbc;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class PlainTransport implements Transport {

    Socket con;
    InputStream in;
    OutputStream out;


    public PlainTransport( String host, int port ) throws IOException {
        con = new Socket( host, port );
        in = con.getInputStream();
        out = con.getOutputStream();
    }


    @Override
    public void sendMessage( byte[] message ) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate( 8 + message.length );
        bb.order( ByteOrder.LITTLE_ENDIAN );
        bb.putLong( message.length );
        bb.put( message );
        out.write( bb.array() );
    }


    @Override
    public byte[] receiveMessage() throws IOException {
        byte[] b = in.readNBytes( 8 );
        if ( b.length != 8 ) {
            if ( b.length == 0 ) { // EOF
                throw new EOFException();
            }
            throw new IOException( "short read" );
        }
        ByteBuffer bb = ByteBuffer.wrap( b );
        bb.order( ByteOrder.LITTLE_ENDIAN ); // TODO Big endian like other network protocols?
        long length = bb.getLong();
        return in.readNBytes( (int) length );
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

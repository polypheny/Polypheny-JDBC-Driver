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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.ProtoFile;

public class PolyBlob implements Blob {

    /*
     * This array should be replaced with an objet capable of either storing a collection of bytes larger than MAX_INT
     * or some kind of streaming mechanism.
     */
    private byte[] binaryValue;
    private PrismInputStream prismInputStream;
    private boolean isStream;
    boolean isFreed;


    public PolyBlob() {
        this.isFreed = false;
    }


    public PolyBlob( ProtoFile protoFile, PolyConnection connection ) {
        this.isFreed = false;
        switch (protoFile.getDataCase()) {
            case BINARY:
                binaryValue = protoFile.getBinary().toByteArray();
                break;
            case STREAM_ID:
                isStream = true;
                //TODO: actually set statement id
                prismInputStream = new PrismInputStream(42, protoFile.getStreamId(), protoFile.getIsForwardOnly(), connection );
        }
    }

    public PolyBlob( byte[] binaryValue) {
        this.binaryValue = binaryValue;
    }


    private long positionToIndex( long position ) {
        return position - 1;
    }


    private long indexToPosition( long index ) {
        return index + 1;
    }


    private int longToInt( long value ) {
        return Math.toIntExact( value );
    }


    private void throwIfPositionOutOfBounds( long position ) throws SQLException {
        throwIfIndexOutOfBounds( positionToIndex( position ) );
    }


    private void throwIfIndexOutOfBounds( long index ) throws SQLException {
        if ( index < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds" );
        }
        if ( index >= binaryValue.length ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds" );
        }
    }


    private void throwIfFreed() throws SQLException {
        if ( isFreed ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Illegal operation on freed blob" );
        }
    }


    @Override
    public long length() throws SQLException {
        if (binaryValue != null) {
            return binaryValue.length;
        }
        try {
            return prismInputStream.available();
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to get BLOB length.", e );
        }
    }


    @Override
    public byte[] getBytes( long pos, int length ) throws SQLException {
        throwIfFreed();
        if (binaryValue != null) {
            throwIfPositionOutOfBounds( pos );
            throwIfPositionOutOfBounds( pos + length - 1 );
            pos = positionToIndex( pos );
            return Arrays.copyOfRange( binaryValue, longToInt( pos ), length );
        }
        try {
            return prismInputStream.getBytes( pos, length );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, e.getMessage(), e );
        }

    }


    @Override
    public InputStream getBinaryStream() throws SQLException {
        throwIfFreed();
        if (binaryValue != null) {
            return new ByteArrayInputStream( binaryValue );
        }
        return prismInputStream;
    }


    @Override
    public long position( byte[] bytes, long start ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not implemented" );
    }


    @Override
    public long position( Blob blob, long start ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not implemented" );
    }


    @Override
    public int setBytes( long pos, byte[] bytes ) throws SQLException {
        return setBytes( pos, bytes, 0, bytes.length );
    }


    @Override
    public int setBytes( long pos, byte[] bytes, int offset, int len ) throws SQLException {
        throwIfFreed();
        if (prismInputStream != null) {
            throw new PrismInterfaceServiceException(PrismInterfaceErrors.STREAM_ERROR, "This blob contains a datastream. Writes to datastreams are not permitted.");
        }
        if ( binaryValue == null ) {
            binaryValue = new byte[len];
        }
        if ( positionToIndex( pos + len ) >= binaryValue.length ) {
            binaryValue = Arrays.copyOf( binaryValue, longToInt( positionToIndex( pos + len ) ) );
        }
        for ( int bytesWritten = 0; bytesWritten < len; bytesWritten++ ) {
            int writeIndex = longToInt( positionToIndex( pos ) ) + bytesWritten;
            binaryValue[writeIndex] = bytes[offset + bytesWritten];
        }
        return len;
    }


    @Override
    public OutputStream setBinaryStream( long pos ) throws SQLException {
        throwIfFreed();
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public void truncate( long len ) throws SQLException {
        throwIfFreed();
        if ( len < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal argument for len" );
        }
        if ( binaryValue != null ) {
            len = Math.min( len, binaryValue.length );
            binaryValue = Arrays.copyOf( binaryValue, longToInt( len ) );
            return;
        }
        throw new PrismInterfaceServiceException(PrismInterfaceErrors.STREAM_ERROR, "This blob already contains a datastream. Truncation of streams not allowed.");

    }


    @Override
    public void free() throws SQLException {
        this.isFreed = true;
    }


    @Override
    public InputStream getBinaryStream( long pos, long len ) throws SQLException {
        throwIfFreed();
        if (binaryValue != null) {
            int from = longToInt( positionToIndex( pos ) );
            int to = longToInt( positionToIndex( pos + len ) );
            byte[] slice = Arrays.copyOfRange( binaryValue, from, to );
            return new ByteArrayInputStream( slice );
        }
        return new PrismInputStream( prismInputStream, pos + len, pos );
    }

}

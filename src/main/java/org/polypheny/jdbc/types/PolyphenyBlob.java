package org.polypheny.jdbc.types;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Arrays;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.SQLErrors;

public class PolyphenyBlob implements Blob {

    /*
     * This array should be replaced with an objet capable of either storing a collection of bytes larger than MAX_INT
     * or some kind of streaming mechanism.
     */
    byte[] value;
    boolean isFreed;


    public PolyphenyBlob() {
        this.isFreed = false;
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
        /* jdbc starts enumeration by one */
        throwIfIndexOutOfBounds( positionToIndex( position ) );
    }


    private void throwIfIndexOutOfBounds( long index ) throws SQLException {
        if ( index < 0 ) {
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Index out of bounds" );
        }
        if ( index >= value.length ) {
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Index out of bounds" );
        }
    }


    private void throwIfFreed() throws SQLException {
        if ( isFreed ) {
            throw new ProtoInterfaceServiceException( SQLErrors.OPERATION_ILLEGAL, "Illegal operation on freed blob" );
        }
    }


    @Override
    public long length() throws SQLException {
        return value.length;
    }


    @Override
    public byte[] getBytes( long pos, int length ) throws SQLException {
        throwIfFreed();
        throwIfPositionOutOfBounds( pos );
        throwIfPositionOutOfBounds( pos + length - 1 );
        return Arrays.copyOfRange( value, longToInt( pos ), length );
    }


    @Override
    public InputStream getBinaryStream() throws SQLException {
        throwIfFreed();
        return new ByteArrayInputStream( value );
    }


    @Override
    public long position( byte[] bytes, long start ) throws SQLException {
        /* Could efficiently be implemented using Knuth-Morris-Pratt-Algorithm */
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
        if ( positionToIndex( pos + len ) >= value.length ) {
            value = Arrays.copyOf( value, longToInt( positionToIndex( pos + len ) ) );
        }
        for ( int bytesWritten = 0; bytesWritten < len; bytesWritten++ ) {
            int writeIndex = longToInt( positionToIndex( pos ) ) + bytesWritten;
            value[writeIndex] = bytes[offset + bytesWritten];
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
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "Illegal argument for len" );
        }
        len = Math.min( len, value.length );
        value = Arrays.copyOf( value, longToInt( len ) );
    }


    @Override
    public void free() throws SQLException {
        this.isFreed = true;
    }


    @Override
    public InputStream getBinaryStream( long pos, long len ) throws SQLException {
        throwIfFreed();
        int from = longToInt( positionToIndex( pos ) );
        int to = longToInt( positionToIndex( pos + len ) );
        byte[] slice = Arrays.copyOfRange( value, from, to );
        return new ByteArrayInputStream( slice );
    }

}

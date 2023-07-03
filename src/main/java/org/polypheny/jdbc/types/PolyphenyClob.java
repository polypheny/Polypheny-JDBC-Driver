package org.polypheny.jdbc.types;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public class PolyphenyClob implements Clob {

    String value;
    boolean isFreed;


    private void throwIfFreed() throws SQLException {
        if ( isFreed ) {
            throw new SQLException( "Illegal operation on freed blob" );
        }
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


    @Override
    public long length() throws SQLException {
        throwIfFreed();
        return value.length();
    }


    @Override
    public String getSubString( long pos, int length ) throws SQLException {
        throwIfFreed();
        int startIndex = longToInt( positionToIndex( pos ) );
        return value.substring( startIndex, length );
    }


    @Override
    public Reader getCharacterStream() throws SQLException {
        throwIfFreed();
        return new StringReader( value );
    }


    @Override
    public InputStream getAsciiStream() throws SQLException {
        throwIfFreed();
        return new ByteArrayInputStream( value.getBytes( StandardCharsets.US_ASCII ) );
    }


    @Override
    public long position( String searchstr, long start ) throws SQLException {
        throwIfFreed();
        int startIndex = longToInt( positionToIndex( start ) );
        return value.indexOf( searchstr, startIndex );
    }


    @Override
    public long position( Clob clob, long l ) throws SQLException {
        throwIfFreed();
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public int setString( long pos, String str ) throws SQLException {
        throwIfFreed();
        replaceSection( longToInt( positionToIndex( pos ) ), str.length(), str );
        return str.length();
    }


    private void replaceSection( int startIndex, int replacementLength, String replacement ) {
        value = value.substring( 0, startIndex ) + replacement + value.substring( startIndex + replacementLength );
    }


    @Override
    public int setString( long pos, String str, int offset, int len ) throws SQLException {
        throwIfFreed();
        int startIndex = longToInt( positionToIndex( offset ) );
        int endIndex = longToInt( positionToIndex( offset + len ) );
        String replacement = str.substring( startIndex, endIndex );
        return setString( pos, replacement );
    }


    @Override
    public OutputStream setAsciiStream( long pos ) throws SQLException {
        throwIfFreed();
        throw new SQLFeatureNotSupportedException( "feature not supported" );
    }


    @Override
    public Writer setCharacterStream( long l ) throws SQLException {
        throwIfFreed();
        throw new SQLFeatureNotSupportedException( "feature not supported" );
    }


    @Override
    public void truncate( long len ) throws SQLException {
        throwIfFreed();
        value = value.substring( 0, longToInt( len ) );
    }


    @Override
    public void free() throws SQLException {
        this.isFreed = true;
    }


    @Override
    public Reader getCharacterStream( long pos, long length ) throws SQLException {
        throwIfFreed();
        int startIndex = longToInt( positionToIndex( pos ) );
        String slice = value.substring( startIndex, startIndex + longToInt( length ) );
        return null;
    }
}

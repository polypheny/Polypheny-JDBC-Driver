/*
 * Copyright 2019-2023 The Polypheny Project
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
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class PolyphenyClob implements Clob, NClob {

    String value;
    boolean isFreed;


    private void throwIfFreed() throws SQLException {
        if ( isFreed ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Illegal operation on freed blob" );
        }
    }


    public PolyphenyClob( String string ) {
        this.isFreed = false;
        this.value = string;
    }


    public PolyphenyClob() {
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


    private void replaceSection( int startIndex, int replacementLength, String replacement ) throws PrismInterfaceServiceException {
        if ( value == null ) {
            if ( startIndex > 0 ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Can't replace section in empty string" );
            }
            value = replacement;
            return;
        }
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

package org.polypheny.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.sql.Clob;
import java.sql.SQLException;

public class PolyphenyClob implements Clob {

    @Override
    public long length() throws SQLException {
        return 0;
    }


    @Override
    public String getSubString( long l, int i ) throws SQLException {
        return null;
    }


    @Override
    public Reader getCharacterStream() throws SQLException {
        return null;
    }


    @Override
    public InputStream getAsciiStream() throws SQLException {
        return null;
    }


    @Override
    public long position( String s, long l ) throws SQLException {
        return 0;
    }


    @Override
    public long position( Clob clob, long l ) throws SQLException {
        return 0;
    }


    @Override
    public int setString( long l, String s ) throws SQLException {
        return 0;
    }


    @Override
    public int setString( long l, String s, int i, int i1 ) throws SQLException {
        return 0;
    }


    @Override
    public OutputStream setAsciiStream( long l ) throws SQLException {
        return null;
    }


    @Override
    public Writer setCharacterStream( long l ) throws SQLException {
        return null;
    }


    @Override
    public void truncate( long l ) throws SQLException {

    }


    @Override
    public void free() throws SQLException {

    }


    @Override
    public Reader getCharacterStream( long l, long l1 ) throws SQLException {
        return null;
    }

}

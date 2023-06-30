package org.polypheny.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

public class PolyphenyBlob implements Blob {

    @Override
    public long length() throws SQLException {
        return 0;
    }


    @Override
    public byte[] getBytes( long l, int i ) throws SQLException {
        return new byte[0];
    }


    @Override
    public InputStream getBinaryStream() throws SQLException {
        return null;
    }


    @Override
    public long position( byte[] bytes, long l ) throws SQLException {
        return 0;
    }


    @Override
    public long position( java.sql.Blob blob, long l ) throws SQLException {
        return 0;
    }


    @Override
    public int setBytes( long l, byte[] bytes ) throws SQLException {
        return 0;
    }


    @Override
    public int setBytes( long l, byte[] bytes, int i, int i1 ) throws SQLException {
        return 0;
    }


    @Override
    public OutputStream setBinaryStream( long l ) throws SQLException {
        return null;
    }


    @Override
    public void truncate( long l ) throws SQLException {

    }


    @Override
    public void free() throws SQLException {

    }


    @Override
    public InputStream getBinaryStream( long l, long l1 ) throws SQLException {
        return null;
    }

}

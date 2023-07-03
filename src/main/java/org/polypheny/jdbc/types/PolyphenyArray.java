package org.polypheny.jdbc.types;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class PolyphenyArray implements Array {

    @Override
    public String getBaseTypeName() throws SQLException {
        return null;
    }


    @Override
    public int getBaseType() throws SQLException {
        return 0;
    }


    @Override
    public Object getArray() throws SQLException {
        return null;
    }


    @Override
    public Object getArray( Map<String, Class<?>> map ) throws SQLException {
        return null;
    }


    @Override
    public Object getArray( long l, int i ) throws SQLException {
        return null;
    }


    @Override
    public Object getArray( long l, int i, Map<String, Class<?>> map ) throws SQLException {
        return null;
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return null;
    }


    @Override
    public ResultSet getResultSet( Map<String, Class<?>> map ) throws SQLException {
        return null;
    }


    @Override
    public ResultSet getResultSet( long l, int i ) throws SQLException {
        return null;
    }


    @Override
    public ResultSet getResultSet( long l, int i, Map<String, Class<?>> map ) throws SQLException {
        return null;
    }


    @Override
    public void free() throws SQLException {

    }

}

package org.polypheny.jdbc.types;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class PolyphenyStruct implements Struct {

    List<Object> attributes;
    String typeName;


    public PolyphenyStruct( String typeName, Object[] attributes ) {
        this.typeName = typeName;
        this.attributes = new ArrayList<>( Arrays.asList( attributes ) );
    }


    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }


    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes.toArray();
    }


    @Override
    public Object[] getAttributes( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }

}

package org.polypheny.jdbc.types;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Struct;
import java.util.Map;

public class PolyphenyStruct implements Struct {

    Map<String, TypedValue> attributes;
    String typeName;

    @Override
    public String getSQLTypeName() throws SQLException {
        return typeName;
    }


    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes.values().stream().map( TypedValue::asObject ).toArray();
    }


    @Override
    public Object[] getAttributes( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Feature not supported");
    }
}

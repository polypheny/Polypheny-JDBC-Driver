package org.polypheny.jdbc.types;

import java.sql.SQLException;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PolyphenyStruct implements Struct {
    List<TypedValue> attributes;

    @Override
    public String getSQLTypeName() throws SQLException {
        return "";
    }


    @Override
    public Object[] getAttributes() throws SQLException {
        return attributes.stream().map( TypedValue::asObject ).toArray(Object[]::new);
    }


    @Override
    public Object[] getAttributes( Map<String, Class<?>> map ) throws SQLException {
        return new Object[0];
    }

}

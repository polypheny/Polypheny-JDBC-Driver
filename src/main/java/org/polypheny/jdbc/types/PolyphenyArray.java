package org.polypheny.jdbc.types;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.polypheny.jdbc.PolyphenyBidirectionalResultSet;
import org.polypheny.jdbc.meta.PolyphenyColumnMeta;
import org.polypheny.jdbc.deserialization.ProtoToJdbcTypeMap;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class PolyphenyArray implements Array {

    private final String protoBaseTypeName;
    private final Object[] elements;


    public PolyphenyArray( String protoBaseTypeName, Object[] elements ) {
        this.protoBaseTypeName = protoBaseTypeName;
        Object[] shiftedElements = new Object[elements.length + 1];
        // shifting array elements one to the right as first value has to be at index 1... duh!
        int endIdx = elements.length + 1;
        for (int i = 1; i < endIdx; i++ ) {
            shiftedElements[i] = elements[i - 1];
        }
        this.elements = shiftedElements;
    }

    public PolyphenyArray( String protoBaseTypeName, List<TypedValue> values ) throws SQLException {
        this.protoBaseTypeName = protoBaseTypeName;
        List<Object> objects = new ArrayList<>();
        for ( TypedValue v : values ) {
            Object object = v.asObject();
            objects.add( object );
        }
        objects.add( 0, TypedValue.fromNull( values.get( 0 ).getJdbcType() ) );
        this.elements = objects.toArray(new Object[0]);
    }

    private int longToInt( long value ) {
        return Math.toIntExact( value );
    }

    @Override
    public String getBaseTypeName(){
        return protoBaseTypeName;
    }


    @Override
    public int getBaseType(){
        return ProtoToJdbcTypeMap.getJdbcTypeFromProto( ProtoValueType.valueOf( protoBaseTypeName ) );
    }


    @Override
    public Object getArray(){
        return elements;
    }


    @Override
    public Object getArray( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public Object getArray( long index, int count ) {
        return Arrays.copyOfRange( elements, longToInt( index ), count );
    }


    @Override
    public Object getArray( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet(0, elements.length);
    }


    @Override
    public ResultSet getResultSet( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSet getResultSet( long index, int count ) throws SQLException {
        int jdbcBaseType = getBaseType();
        ArrayList<PolyphenyColumnMeta> columnMetas = new ArrayList<>();
        columnMetas.add( PolyphenyColumnMeta.fromSpecification( 0, "INDEX", "ARRAY", Types.INTEGER ) );
        columnMetas.add( PolyphenyColumnMeta.fromSpecification( 1, "VALUE", "ARRAY", jdbcBaseType ) );
        ArrayList<ArrayList<TypedValue>> rows = new ArrayList<>();
        for (int i = 1; i < elements.length; i++) {
            ArrayList<TypedValue> currentRow = new ArrayList<>();
            currentRow.add( TypedValue.fromInt( i ) );
            currentRow.add( TypedValue.fromObject( elements[i], jdbcBaseType ) );
            rows.add( currentRow );
        }
        return new PolyphenyBidirectionalResultSet(columnMetas, rows);
    }


    @Override
    public ResultSet getResultSet( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public void free() {
    }

}

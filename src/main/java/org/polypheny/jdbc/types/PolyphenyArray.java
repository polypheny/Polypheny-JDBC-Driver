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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.polypheny.jdbc.PolyphenyResultSet;
import org.polypheny.jdbc.meta.PolyphenyColumnMeta;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class PolyphenyArray implements Array {

    private final String protoBaseTypeName;
    private final Object[] elements;


    public PolyphenyArray( String protoBaseTypeName, Object[] elements ) {
        this.protoBaseTypeName = protoBaseTypeName;
        Object[] shiftedElements = new Object[elements.length];
        int endIdx = elements.length;
        System.arraycopy( elements, 0, shiftedElements, 0, endIdx );
        this.elements = shiftedElements;
    }


    public PolyphenyArray( String protoBaseTypeName, List<TypedValue> values ) throws SQLException {
        this.protoBaseTypeName = protoBaseTypeName;
        List<Object> objects = new ArrayList<>();
        for ( TypedValue v : values ) {
            Object object = v.asObject();
            objects.add( object );
        }
        this.elements = objects.toArray( new Object[0] );
    }


    private int longToInt( long value ) {
        return Math.toIntExact( value );
    }


    @Override
    public String getBaseTypeName() {
        return protoBaseTypeName;
    }


    @Override
    public int getBaseType() {
        return ProtoToJdbcTypeMap.getJdbcTypeFromProto( ProtoPolyType.valueOf( protoBaseTypeName ) );
    }


    @Override
    public Object getArray() {
        return elements;
    }


    @Override
    public Object getArray( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public Object getArray( long index, int count ) {
        return Arrays.copyOfRange( elements, longToInt( index - 1 ), count );
    }


    @Override
    public Object getArray( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet( 0, elements.length );
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
        for ( int i = 1; i < elements.length; i++ ) {
            ArrayList<TypedValue> currentRow = new ArrayList<>();
            currentRow.add( TypedValue.fromInteger( i ) );
            currentRow.add( TypedValue.fromObject( elements[i], jdbcBaseType ) );
            rows.add( currentRow );
        }
        return new PolyphenyResultSet( columnMetas, rows );
    }


    @Override
    public ResultSet getResultSet( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public void free() {
    }

}

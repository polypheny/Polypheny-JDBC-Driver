/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.PolyphenyResultSet;
import org.polypheny.jdbc.meta.PolyphenyColumnMeta;
import org.polypheny.prism.ProtoPolyType;

public class PolyArray implements Array {

    private final String protoBaseTypeName;
    private final Object[] elements;
    private final List<TypedValue> wireValues;


    public PolyArray( String protoBaseTypeName, Object[] elements ) {
        this.protoBaseTypeName = protoBaseTypeName;
        this.elements = elements.clone();
        this.wireValues = null;
    }


    public PolyArray( String protoBaseTypeName, List<TypedValue> values ) throws SQLException {
        this.protoBaseTypeName = protoBaseTypeName;
        this.wireValues = new ArrayList<>( values );
        this.elements = new Object[wireValues.size()];
        for ( int i = 0; i < elements.length; i++ ) {
            elements[i] = wireValues.get( i ).asObject();
        }
    }


    private int getSliceOffset( long index, int count ) throws SQLException {
        if ( index < 1 || index > (long) elements.length + 1 || count < 0 || count > elements.length - (index - 1) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Array slice is outside the array." );
        }
        return (int) (index - 1);
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
        return elements.clone();
    }


    @Override
    public Object getArray( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public Object getArray( long index, int count ) throws SQLException {
        int offset = getSliceOffset( index, count );
        return Arrays.copyOfRange( elements, offset, offset + count );
    }


    @Override
    public Object getArray( long index, int count, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSet getResultSet() throws SQLException {
        return getResultSet( 1, elements.length );
    }


    @Override
    public ResultSet getResultSet( Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSet getResultSet( long index, int count ) throws SQLException {
        int offset = getSliceOffset( index, count );
        int jdbcBaseType = getBaseType();
        List<PolyphenyColumnMeta> columnMetas = Arrays.asList(
                PolyphenyColumnMeta.fromSpecification( 0, "INDEX", "ARRAY", Types.INTEGER ),
                PolyphenyColumnMeta.fromSpecification( 1, "VALUE", "ARRAY", jdbcBaseType ) );
        List<List<TypedValue>> rows = new ArrayList<>( count );
        for ( int i = offset; i < offset + count; i++ ) {
            TypedValue value = wireValues == null
                    ? TypedValue.fromObject( elements[i], jdbcBaseType )
                    : wireValues.get( i );
            rows.add( Arrays.asList( TypedValue.fromInteger( i + 1 ), value ) );
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

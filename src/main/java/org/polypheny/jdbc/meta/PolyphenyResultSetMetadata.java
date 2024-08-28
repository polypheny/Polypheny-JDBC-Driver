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

package org.polypheny.jdbc.meta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class PolyphenyResultSetMetadata implements ResultSetMetaData {

    private final List<PolyphenyColumnMeta> columnMetas;
    private final Map<String, Integer> columnIndexes;


    public PolyphenyResultSetMetadata( List<PolyphenyColumnMeta> columnMetas ) {
        this.columnMetas = columnMetas;
        this.columnIndexes = this.columnMetas.stream().collect( Collectors.toMap( c -> c.getColumnName().toLowerCase(), c -> c.getOrdinal() + 1, ( m, n ) -> n ) );

    }


    private PolyphenyColumnMeta getMeta( int columnIndex ) throws SQLException {
        try {
            return columnMetas.get( columnIndex - 1 );
        } catch ( IndexOutOfBoundsException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Column index out of bounds", e );
        }
    }


    public int getColumnIndexFromLabel( String columnLabel ) throws SQLException {
        Integer columnIndex = columnIndexes.get( columnLabel.toLowerCase() );
        if ( columnIndex == null ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.COLUMN_NOT_EXISTS, "Invalid column label: " + columnLabel );
        }
        return columnIndex;
    }


    @Override
    public int getColumnCount() throws SQLException {
        return columnMetas.size();
    }


    @Override
    public boolean isAutoIncrement( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isAutoIncrement();
    }


    @Override
    public boolean isCaseSensitive( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isCaseSensitive();
    }


    @Override
    public boolean isSearchable( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isSearchable();
    }


    @Override
    public boolean isCurrency( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isCurrency();
    }


    @Override
    public int isNullable( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getNullable();
    }


    @Override
    public boolean isSigned( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isSigned();
    }


    @Override
    public int getColumnDisplaySize( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getDisplaySize();
    }


    @Override
    public String getColumnLabel( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getColumnLabel();
    }


    @Override
    public String getColumnName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getColumnName();
    }


    @Override
    public String getSchemaName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getNamespace();
    }


    @Override
    public int getPrecision( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getPrecision();
    }


    @Override
    public int getScale( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getScale();
    }


    @Override
    public String getTableName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getTableName();
    }


    @Override
    public String getCatalogName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getCatalogName();
    }


    @Override
    public int getColumnType( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getSqlType();
    }


    @Override
    public String getColumnTypeName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getPolyphenyFieldTypeName();
    }


    @Override
    public boolean isReadOnly( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isReadOnly();
    }


    @Override
    public boolean isWritable( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isWritable();
    }


    @Override
    public boolean isDefinitelyWritable( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).isDefinitelyWritable();
    }


    @Override
    public String getColumnClassName( int columnIndex ) throws SQLException {
        return getMeta( columnIndex ).getColumnClassName();
    }


    @Override
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass );
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) {
        return aClass.isInstance( this );
    }

}

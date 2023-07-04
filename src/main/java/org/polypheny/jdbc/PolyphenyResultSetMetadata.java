package org.polypheny.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.jdbc.proto.ColumnMeta;
import org.polypheny.jdbc.utils.MetaUtils;

public class PolyphenyResultSetMetadata implements ResultSetMetaData {

    ArrayList<PolyphenyColumnMeta> columnMetas;
    Map<String, Integer> columnIndexes;


    public PolyphenyResultSetMetadata( List<ColumnMeta> columnMetas ) {
        ArrayList<PolyphenyColumnMeta> polyphenyColumnMetas = MetaUtils.buildColumnMetas( columnMetas );
        this.columnIndexes = columnMetas.stream().collect( Collectors.toMap( ColumnMeta::getColumnLabel, m -> m.getColumnIndex() + 1, ( m, n ) -> n ) );
    }


    public PolyphenyResultSetMetadata( ArrayList<PolyphenyColumnMeta> columnMetaData ) {
        this.columnMetas = columnMetaData;
        this.columnIndexes = columnMetaData.stream().collect( Collectors.toMap( PolyphenyColumnMeta::getColumnLabel, m -> m.getOrdinal() + 1, ( m, n ) -> n ) );

    }


    private PolyphenyColumnMeta getMeta( int columnIndex ) throws SQLException {
        try {
            return columnMetas.get( columnIndex - 1 );
        } catch ( IndexOutOfBoundsException e ) {
            throw new SQLException( "Column index out of bounds" );
        }
    }


    public int getIndexFromLabel( String columnName ) throws SQLException {
        Integer columnIndex = columnIndexes.get( columnName );
        if ( columnIndex == null ) {
            throw new SQLException( "Invalid column name: " + columnName );
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
        return getMeta( columnIndex ).getTableName();
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
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }

}

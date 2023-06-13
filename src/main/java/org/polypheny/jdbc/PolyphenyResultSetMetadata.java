package org.polypheny.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.checkerframework.checker.units.qual.A;
import org.polypheny.jdbc.proto.ColumnMeta;

public class PolyphenyResultSetMetadata implements ResultSetMetaData {
    ArrayList<ColumnMeta> columnMetas;

    public PolyphenyResultSetMetadata( List<ColumnMeta> columnMetas ) {
        this.columnMetas = new ArrayList<>(columnMetas);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnMetas.size();
    }


    @Override
    public boolean isAutoIncrement( int i ) throws SQLException {
        return false;
    }


    @Override
    public boolean isCaseSensitive( int i ) throws SQLException {
        return false;
    }


    @Override
    public boolean isSearchable( int i ) throws SQLException {
        return false;
    }


    @Override
    public boolean isCurrency( int i ) throws SQLException {
        return false;
    }


    @Override
    public int isNullable( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get(columnIndex).getIsNullable() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
    }


    @Override
    public boolean isSigned( int i ) throws SQLException {
        return false;
    }


    @Override
    public int getColumnDisplaySize( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get(columnIndex).getDisplaySize();
    }


    @Override
    public String getColumnLabel( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get(columnIndex).getColumnLabel();
    }


    @Override
    public String getColumnName( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get( columnIndex ).getColumnName();
    }


    @Override
    public String getSchemaName( int i ) throws SQLException {
        return null;
    }


    @Override
    public int getPrecision( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get( columnIndex ).getPrecision();
    }


    @Override
    public int getScale( int i ) throws SQLException {
        return 0;
    }


    @Override
    public String getTableName( int columnIndex ) throws SQLException {
        columnIndex--;
        return columnMetas.get( columnIndex ).getTableName();
    }


    @Override
    public String getCatalogName( int i ) throws SQLException {
        return null;
    }


    @Override
    public int getColumnType( int i ) throws SQLException {
        return 0;
    }


    @Override
    public String getColumnTypeName( int i ) throws SQLException {
        return null;
    }


    @Override
    public boolean isReadOnly( int i ) throws SQLException {
        return false;
    }


    @Override
    public boolean isWritable( int i ) throws SQLException {
        return false;
    }


    @Override
    public boolean isDefinitelyWritable( int i ) throws SQLException {
        return false;
    }


    @Override
    public String getColumnClassName( int i ) throws SQLException {
        return null;
    }


    @Override
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        return null;
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) throws SQLException {
        return false;
    }

}

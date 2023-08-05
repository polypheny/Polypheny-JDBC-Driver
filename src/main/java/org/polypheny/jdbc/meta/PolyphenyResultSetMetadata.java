package org.polypheny.jdbc.meta;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.proto.ColumnMeta;

public class PolyphenyResultSetMetadata implements ResultSetMetaData {

    private ArrayList<PolyphenyColumnMeta> columnMetas;
    private Map<String, Integer> columnIndexes;


    public PolyphenyResultSetMetadata( List<ColumnMeta> columnMetas ) {
        this.columnMetas = MetaUtils.buildColumnMetas( columnMetas );
        this.columnIndexes = this.columnMetas.stream().collect( Collectors.toMap( PolyphenyColumnMeta::getColumnLabel, c -> c.getOrdinal() + 1, ( m, n ) -> n ) );
    }


    public PolyphenyResultSetMetadata( ArrayList<PolyphenyColumnMeta> columnMetas ) {
        this.columnMetas = columnMetas;
        this.columnIndexes = this.columnMetas.stream().collect( Collectors.toMap( PolyphenyColumnMeta::getColumnLabel, c -> c.getOrdinal() + 1, ( m, n ) -> n ) );

    }


    private PolyphenyColumnMeta getMeta( int columnIndex ) throws SQLException {
        try {
            return columnMetas.get( columnIndex - 1 );
        } catch ( IndexOutOfBoundsException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.VALUE_ILLEGAL, "Column index out of bounds", e );
        }
    }


    public int getColumnIndexFromLabel( String columnLabel ) throws SQLException {
        Integer columnIndex = columnIndexes.get( columnLabel );
        if ( columnIndex == null ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.COLUMN_NOT_EXISTS,  "Invalid column label: " + columnLabel );
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
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.WRAPPER_INCORRECT_TYPE, "Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return aClass.isInstance(this);

    }

}

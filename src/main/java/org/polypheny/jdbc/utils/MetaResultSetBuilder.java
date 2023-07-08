package org.polypheny.jdbc.utils;

import com.google.protobuf.GeneratedMessageV3;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyphenyBidirectionalResultSet;
import org.polypheny.jdbc.PolyphenyColumnMeta;
import org.polypheny.jdbc.proto.Column;
import org.polypheny.jdbc.proto.ColumnsResponse;
import org.polypheny.jdbc.proto.Database;
import org.polypheny.jdbc.proto.DatabasesResponse;
import org.polypheny.jdbc.proto.ExportedKeysResponse;
import org.polypheny.jdbc.proto.ForeignKey;
import org.polypheny.jdbc.proto.ImportedKeysResponse;
import org.polypheny.jdbc.proto.Namespace;
import org.polypheny.jdbc.proto.NamespacesResponse;
import org.polypheny.jdbc.proto.PrimaryKey;
import org.polypheny.jdbc.proto.PrimaryKeysResponse;
import org.polypheny.jdbc.proto.Table;
import org.polypheny.jdbc.proto.TableType;
import org.polypheny.jdbc.proto.TableTypesResponse;
import org.polypheny.jdbc.proto.TablesResponse;
import org.polypheny.jdbc.types.TypedValue;

public class MetaResultSetBuilder {

    private static final Function NULL_FUNCTION = o -> null;

    private static final List<Parameter<ForeignKey>> FOREIGN_KEY_PARAMETERS = Arrays.asList(
            new Parameter<>( "PKTABLE_CAT", Types.VARCHAR, nullIfFalse( ForeignKey::getReferencedDatabaseName, ForeignKey::hasReferencedDatabaseName ) ),
            new Parameter<>( "PKTABLE_SCHEM", Types.VARCHAR, nullIfFalse( ForeignKey::getReferencedNamespaceName, ForeignKey::hasReferencedNamespaceName ) ),
            new Parameter<>( "PKTABLE_NAME", Types.VARCHAR, ForeignKey::getReferencedTableName ),
            new Parameter<>( "PKCOLUMN_NAME", Types.VARCHAR, ForeignKey::getReferencedColumnName ),
            new Parameter<>( "FKTABLE_CAT", Types.VARCHAR, nullIfFalse( ForeignKey::getForeignDatabaseName, ForeignKey::hasForeignDatabaseName ) ),
            new Parameter<>( "FKTABLE_SCHEM", Types.VARCHAR, nullIfFalse( ForeignKey::getForeignNamespaceName, ForeignKey::hasForeignNamespaceName ) ),
            new Parameter<>( "FKTABLE_NAME", Types.VARCHAR, ForeignKey::getForeignTableName ),
            new Parameter<>( "FKCOLUMN_NAME", Types.VARCHAR, ForeignKey::getForeignColumnName ),
            new Parameter<>( "KEY_SEQ", Types.SMALLINT, integerAsShort( ForeignKey::getSequenceIndex ) ),
            new Parameter<>( "UPDATE_RULE", Types.SMALLINT, integerAsShort( ForeignKey::getUpdateRule ) ),
            new Parameter<>( "DELETE_RULE", Types.SMALLINT, integerAsShort( ForeignKey::getDeleteRule ) ),
            new Parameter<>( "FK_NAME", Types.VARCHAR, ForeignKey::getKeyName ),
            new Parameter<ForeignKey>( "PK_NAME", Types.VARCHAR, NULL_FUNCTION ),
            new Parameter<ForeignKey>( "DEFERRABILITY", Types.SMALLINT, NULL_FUNCTION )
    );


    private static <T extends GeneratedMessageV3> PolyphenyBidirectionalResultSet buildResultSet( String entityName, List<T> messages, List<Parameter<T>> parameters ) {
        ArrayList<PolyphenyColumnMeta> columnMetas = buildMetas( entityName, parameters );
        ArrayList<ArrayList<TypedValue>> rows = buildRows( messages, parameters );
        return new PolyphenyBidirectionalResultSet( columnMetas, rows );
    }


    private static <T extends GeneratedMessageV3> ArrayList<PolyphenyColumnMeta> buildMetas( String entityName, List<Parameter<T>> parameters ) {
        AtomicInteger ordinal = new AtomicInteger();
        return parameters.stream()
                .map( p -> PolyphenyColumnMeta.fromSpecification( ordinal.getAndIncrement(), p.name, entityName, p.jdbcType ) )
                .collect( Collectors.toCollection( ArrayList::new ) );
    }


    private static <T extends GeneratedMessageV3> ArrayList<ArrayList<TypedValue>> buildRows( List<T> messages, List<Parameter<T>> parameters ) {
        return messages.stream()
                .map( p -> buildRow( p, parameters ) )
                .collect( Collectors.toCollection( ArrayList::new ) );
    }


    private static <T extends GeneratedMessageV3> ArrayList<TypedValue> buildRow( T message, List<Parameter<T>> parameters ) {
        return parameters.stream()
                .map( p -> p.retrieveFrom( message ) )
                .collect( Collectors.toCollection( ArrayList::new ) );
    }


    private static <T extends GeneratedMessageV3> Function<T, Object> nullIfFalse( Function<T, Object> accessor, Function<T, Boolean> booleanFunction ) {
        return message -> {
            if ( booleanFunction.apply( message ) ) {
                return accessor.apply( message );
            }
            return null;
        };
    }


    private static <T extends GeneratedMessageV3> Function<T, Object> integerAsShort( Function<T, Object> accessor ) {
        return message -> {
            Object value = accessor.apply( message );
            if ( value instanceof Integer ) {
                return ((Integer) value).shortValue();
            }
            throw new IllegalArgumentException( "Can't convert this value to a short" );
        };
    }


    public static ResultSet buildFromTablesResponse( TablesResponse tablesResponse ) {
        return buildResultSet(
                "TABLES",
                tablesResponse.getTablesList(),
                Arrays.asList(
                        new Parameter<>( "TABLE_CAT", Types.VARCHAR, Table::getSourceDatabaseName ),
                        new Parameter<>( "TABLE_SCHEM", Types.VARCHAR, Table::getNamespaceName ),
                        new Parameter<>( "TABLE_NAME", Types.VARCHAR, Table::getTableName ),
                        new Parameter<>( "TABLE_TYPE", Types.VARCHAR, Table::getTableType ),
                        new Parameter<Table>( "REMARKS", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Table>( "TYPE_CAT", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Table>( "TYPE_SCHEM", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Table>( "TYPE_NAME", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Table>( "SELF_REFERENCING_COL_NAME", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Table>( "REF_GENERATION", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<>( "OWNER", Types.VARCHAR, Table::getOwnerName )
                )
        );
    }


    public static ResultSet buildFromTableTypesResponse( TableTypesResponse tableTypesResponse ) {
        return buildResultSet(
                "TABLE_TYPES",
                tableTypesResponse.getTableTypesList(),
                Collections.singletonList(
                        new Parameter<>( "TABLE_TYPE", Types.VARCHAR, TableType::getTableType )
                )
        );
    }


    public static ResultSet buildFromNamespacesResponse( NamespacesResponse namespacesResponse ) {
        return buildResultSet(
                "NAMESPACES",
                namespacesResponse.getNamespacesList(),
                Arrays.asList(
                        new Parameter<>( "TABLE_SCHEM", Types.VARCHAR, Namespace::getNamespaceName ),
                        new Parameter<>( "TABLE_CATALOG", Types.VARCHAR, Namespace::getDatabaseName ),
                        new Parameter<>( "OWNER", Types.VARCHAR, Namespace::getOwnerName ),
                        new Parameter<>( "SCHEMA_TYPE", Types.VARCHAR, nullIfFalse( Namespace::getNamespaceName, Namespace::hasNamespaceType ) )
                )
        );
    }


    public static ResultSet buildFromColumnsResponse( ColumnsResponse columnsResponse ) {
        return buildResultSet(
                "COLUMNS",
                columnsResponse.getColumnsList(),
                Arrays.asList(
                        new Parameter<>( "TABLE_CAT", Types.VARCHAR, Column::getDatabaseName ),
                        new Parameter<>( "TABLE_SCHEM", Types.VARCHAR, Column::getNamespaceName ),
                        new Parameter<>( "TABLE_NAME", Types.VARCHAR, Column::getTableName ),
                        new Parameter<>( "COLUMN_NAME", Types.INTEGER, Column::getColumnName ),
                        new Parameter<>( "DATA_TYPE", Types.VARCHAR, m -> TypedValueUtils.getJdbcTypeFromPolyTypeName( m.getTypeName() ) ),
                        new Parameter<>( "TYPE_NAME", Types.INTEGER, Column::getTypeName ),
                        new Parameter<>( "COLUMN_SIZE", Types.INTEGER, Column::getTypeLength ),
                        new Parameter<Column>( "BUFFER_LENGTH", Types.INTEGER, NULL_FUNCTION ),
                        new Parameter<>( "DECIMAL_DIGITS", Types.INTEGER, Column::getTypeScale ),
                        new Parameter<Column>( "NUM_PREC_RADIX", Types.INTEGER, NULL_FUNCTION ),
                        new Parameter<>( "NULLABLE", Types.VARCHAR, m -> m.getIsNullable() ? 1 : 0 ),
                        new Parameter<>( "REMARKS", Types.VARCHAR, m -> "" ),
                        new Parameter<>( "COLUMN_DEF", Types.VARCHAR, nullIfFalse( Column::getDefaultValueAsString, Column::hasDefaultValueAsString ) ),
                        new Parameter<Column>( "SQL_DATA_TYPE", Types.INTEGER, NULL_FUNCTION ),
                        new Parameter<Column>( "SQL_DATETIME_SUB", Types.INTEGER, NULL_FUNCTION ),
                        new Parameter<Column>( "CHAR_OCTET_LENGTH", Types.INTEGER, NULL_FUNCTION ),
                        new Parameter<>( "ORDINAL_POSITION", Types.INTEGER, Column::getColumnIndex ),
                        new Parameter<>( "IS_NULLABLE", Types.VARCHAR, m -> m.getIsNullable() ? "YES" : "NO" ),
                        new Parameter<Column>( "SCOPE_CATALOG", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Column>( "SCOPE_SCHEMA", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Column>( "SCOPE_TABLE", Types.VARCHAR, NULL_FUNCTION ),
                        new Parameter<Column>( "SOURCE_DATA_TYPE", Types.SMALLINT, NULL_FUNCTION ),
                        new Parameter<>( "IS_AUTOINCREMENT", Types.VARCHAR, m -> "No" ),
                        new Parameter<>( "IS_GENERATEDCOLUMN", Types.VARCHAR, m -> "No" ),
                        new Parameter<>( "COLLATION", Types.VARCHAR, nullIfFalse( Column::getCollation, Column::hasCollation ) )
                )
        );
    }


    public static ResultSet buildFromPrimaryKeyResponse( PrimaryKeysResponse primaryKeysResponse ) {
        return buildResultSet(
                "PRIMARY_KEYS",
                primaryKeysResponse.getPrimaryKeysList(),
                Arrays.asList(
                        new Parameter<>( "TABLE_CAT", Types.VARCHAR, nullIfFalse( PrimaryKey::getDatabaseName, PrimaryKey::hasDatabaseName ) ),
                        new Parameter<>( "TABLE_SCHEM", Types.VARCHAR, nullIfFalse( PrimaryKey::getNamespaceName, PrimaryKey::hasNamespaceName ) ),
                        new Parameter<>( "TABLE_NAME", Types.VARCHAR, PrimaryKey::getTableName ),
                        new Parameter<>( "COLUMN_NAME", Types.VARCHAR, PrimaryKey::getColumnName ),
                        new Parameter<>( "KEY_SEQ", Types.SMALLINT, integerAsShort( PrimaryKey::getSequenceIndex ) ),
                        new Parameter<PrimaryKey>( "PK_NAME", Types.VARCHAR, NULL_FUNCTION )
                )
        );
    }


    public static ResultSet fromDatabasesResponse( DatabasesResponse databasesResponse ) {
        return buildResultSet(
                "CATALOGS",
                databasesResponse.getDatabasesList(),
                Arrays.asList(
                        new Parameter<>( "TABLE_CAT", Types.VARCHAR, Database::getDatabaseName ),
                        new Parameter<>( "OWNER", Types.VARCHAR, Database::getOwnerName ),
                        new Parameter<>( "DEFAULT_SCHEMA", Types.VARCHAR, Database::getDefaultNamespaceName )
                )
        );
    }


    public static ResultSet fromImportedKeysResponse( ImportedKeysResponse importedKeysResponse ) {
        return buildResultSet(
                "IMPORTED_KEYS",
                importedKeysResponse.getImportedKeysList(),
                FOREIGN_KEY_PARAMETERS

        );
    }


    public static ResultSet fromExportedKeysResponse( ExportedKeysResponse exportedKeysResponse ) {
        return buildResultSet(
                "EXPORTED_KEYS",
                exportedKeysResponse.getExportedKeysList(),
                FOREIGN_KEY_PARAMETERS
        );
    }


    static class Parameter<T extends GeneratedMessageV3> {

        private final String name;
        private final int jdbcType;
        private final Function<T, Object> accessFunction;


        Parameter( String name, int jdbcType, Function<T, Object> acessor ) {
            this.name = name;
            this.jdbcType = jdbcType;
            this.accessFunction = acessor;
        }


        TypedValue retrieveFrom( T message ) {
            return new TypedValue( jdbcType, accessFunction.apply( message ) );
        }

    }

}



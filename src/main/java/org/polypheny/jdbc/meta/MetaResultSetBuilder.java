package org.polypheny.jdbc.meta;

import org.polypheny.jdbc.PolyphenyBidirectionalResultSet;
import org.polypheny.jdbc.proto.*;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.TypedValueUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

public class MetaResultSetBuilder {

    private static final Function DUMMY_ACCESSOR = a -> "Dummy value: Accessor not implemented";

    private static <T> PolyphenyBidirectionalResultSet buildEmptyResultSet(String entityName, List<Parameter<T>> parameters) throws SQLException {
        ArrayList<PolyphenyColumnMeta> columnMetas = buildMetas(entityName, parameters);
        ArrayList<ArrayList<TypedValue>> rows = new ArrayList<>();
        return new PolyphenyBidirectionalResultSet(columnMetas, rows);
    }

    private static <T> PolyphenyBidirectionalResultSet buildResultSet(String entityName, List<T> messages, List<Parameter<T>> parameters) throws SQLException {
        ArrayList<PolyphenyColumnMeta> columnMetas = buildMetas(entityName, parameters);
        ArrayList<ArrayList<TypedValue>> rows = buildRows(messages, parameters);
        return new PolyphenyBidirectionalResultSet(columnMetas, rows);
    }


    private static <T> ArrayList<PolyphenyColumnMeta> buildMetas(String entityName, List<Parameter<T>> parameters) {
        AtomicInteger ordinal = new AtomicInteger();
        return parameters.stream()
                .map(p -> PolyphenyColumnMeta.fromSpecification(ordinal.getAndIncrement(), p.name, entityName, p.jdbcType))
                .collect(toCollection(ArrayList::new));
    }


    private static <T> ArrayList<ArrayList<TypedValue>> buildRows(List<T> messages, List<Parameter<T>> parameters) throws SQLException {
        ArrayList<ArrayList<TypedValue>> arrayLists = new ArrayList<>();
        for (T p : messages) {
            ArrayList<TypedValue> typedValues = buildRow(p, parameters);
            arrayLists.add(typedValues);
        }
        return arrayLists;
    }


    private static <T> ArrayList<TypedValue> buildRow(T message, List<Parameter<T>> parameters) throws SQLException {
        ArrayList<TypedValue> typedValues = new ArrayList<>();
        for (Parameter<T> p : parameters) {
            TypedValue typedValue = p.retrieveFrom(message);
            typedValues.add(typedValue);
        }
        return typedValues;
    }


    private static <T> Function<T, Object> nullIfFalse(Function<T, Object> accessor, Function<T, Boolean> booleanFunction) {
        return message -> {
            if (booleanFunction.apply(message)) {
                return accessor.apply(message);
            }
            return null;
        };
    }


    private static <T> Function<T, Object> integerAsShort(Function<T, Object> accessor) {
        return message -> {
            Object value = accessor.apply(message);
            if (value instanceof Integer) {
                return ((Integer) value).shortValue();
            }
            throw new IllegalArgumentException("Can't convert this value to a short");
        };
    }


    public static ResultSet buildFromTables(List<Table> tables) throws SQLException {
        return buildResultSet(
                "TABLES",
                tables,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, Table::getSourceDatabaseName),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, Table::getNamespaceName),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, Table::getTableName),
                        new Parameter<>("TABLE_TYPE", Types.VARCHAR, Table::getTableType),
                        new Parameter<>("REMARKS", Types.VARCHAR, p -> null),
                        new Parameter<>("TYPE_CAT", Types.VARCHAR, p -> null),
                        new Parameter<>("TYPE_SCHEM", Types.VARCHAR, p -> null),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, p -> null),
                        new Parameter<>("SELF_REFERENCING_COL_NAME", Types.VARCHAR, p -> null),
                        new Parameter<>("REF_GENERATION", Types.VARCHAR, p -> null),
                        new Parameter<>("OWNER", Types.VARCHAR, Table::getOwnerName)
                )
        );
    }


    public static ResultSet buildFromTableTypes(List<TableType> tableTypes) throws SQLException {
        return buildResultSet(
                "TABLE_TYPES",
                tableTypes,
                Collections.singletonList(
                        new Parameter<>("TABLE_TYPE", Types.VARCHAR, TableType::getTableType)
                )
        );
    }


    public static ResultSet buildFromNamespaces(List<Namespace> namespaces) throws SQLException {
        return buildResultSet(
                "NAMESPACES",
                namespaces,
                Arrays.asList(
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, Namespace::getNamespaceName),
                        new Parameter<>("TABLE_CATALOG", Types.VARCHAR, Namespace::getDatabaseName),
                        new Parameter<>("OWNER", Types.VARCHAR, Namespace::getOwnerName),
                        new Parameter<>("SCHEMA_TYPE", Types.VARCHAR, nullIfFalse(Namespace::getNamespaceName, Namespace::hasNamespaceType))
                )
        );
    }


    public static ResultSet buildFromColumns(List<Column> columns) throws SQLException {
        return buildResultSet(
                "COLUMNS",
                columns,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, Column::getDatabaseName),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, Column::getNamespaceName),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, Column::getTableName),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, Column::getColumnName),
                        new Parameter<>("DATA_TYPE", Types.VARCHAR, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName(p.getTypeName())),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, Column::getTypeName),
                        new Parameter<>("COLUMN_SIZE", Types.INTEGER, nullIfFalse(Column::getTypeLength, Column::hasTypeLength)),
                        new Parameter<>("BUFFER_LENGTH", Types.INTEGER, p -> null),
                        new Parameter<>("DECIMAL_DIGITS", Types.INTEGER, nullIfFalse(Column::getTypeScale, Column::hasTypeScale)),
                        new Parameter<>("NUM_PREC_RADIX", Types.INTEGER, p -> null),
                        new Parameter<>("NULLABLE", Types.VARCHAR, p -> p.getIsNullable() ? 1 : 0),
                        new Parameter<>("REMARKS", Types.VARCHAR, p -> null),
                        new Parameter<>("COLUMN_DEF", Types.VARCHAR, nullIfFalse(Column::getDefaultValueAsString, Column::hasDefaultValueAsString)),
                        new Parameter<>("SQL_DATA_TYPE", Types.INTEGER, p -> null),
                        new Parameter<>("SQL_DATETIME_SUB", Types.INTEGER, p -> null),
                        new Parameter<>("CHAR_OCTET_LENGTH", Types.INTEGER, p -> null),
                        new Parameter<>("ORDINAL_POSITION", Types.VARCHAR, Column::getColumnIndex),
                        new Parameter<>("IS_NULLABLE", Types.VARCHAR, p -> p.getIsNullable() ? "YES" : "NO"),
                        new Parameter<>("SCOPE_CATALOG", Types.VARCHAR, p -> null),
                        new Parameter<>("SCOPE_SCHEMA", Types.VARCHAR, p -> null),
                        new Parameter<>("SCOPE_TABLE", Types.VARCHAR, p -> null),
                        new Parameter<>("SOURCE_DATA_TYPE", Types.SMALLINT, p -> null),
                        new Parameter<>("IS_AUTOINCREMENT", Types.VARCHAR, p -> "No"),
                        new Parameter<>("IS_GENERATEDCOLUMN", Types.VARCHAR, p -> "No"),
                        new Parameter<>("COLLATION", Types.VARCHAR, nullIfFalse(Column::getCollation, Column::hasCollation))
                )
        );
    }


    public static ResultSet buildFromPrimaryKeys(List<PrimaryKey> primaryKeys) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = primaryKeys.stream()
                .map(MetaResultSetBuilder::expandPrimaryKey)
                .flatMap(List::stream)
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                "PRIMARY_KEYS",
                metaColumns,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, p -> p.getValue(0)),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, p -> p.getValue(1)),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, p -> p.getValue(2)),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, p -> p.getValue(3)),
                        new Parameter<>("KEY_SEQ", Types.SMALLINT, p -> p.getValue(4)),
                        new Parameter<>("PK_NAME", Types.VARCHAR, p -> p.getValue(5))
                )
        );
    }

    private static List<GenericMetaContainer> expandPrimaryKey(PrimaryKey primaryKey) {
        AtomicInteger sequenceIndex = new AtomicInteger();
        return primaryKey.getColumnsList().stream().map(c -> new GenericMetaContainer(
                c.getDatabaseName(),
                c.getNamespaceName(),
                c.getTableName(),
                c.getColumnName(),
                sequenceIndex.getAndIncrement(),
                primaryKey.getNamespaceName()
        )).collect(Collectors.toList());
    }


    public static ResultSet buildFromDatabases(List<Database> databases) throws SQLException {
        return buildResultSet(
                "CATALOGS",
                databases,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, Database::getDatabaseName),
                        new Parameter<>("OWNER", Types.VARCHAR, Database::getOwnerName),
                        new Parameter<>("DEFAULT_SCHEMA", Types.VARCHAR, Database::getDefaultNamespaceName)
                )
        );
    }

    public static ResultSet buildFromImportedKeys(List<ForeignKey> foreignKeys) throws SQLException {
        return buildFromForeignKeys(foreignKeys, "IMPORTED_KEYS");
    }

    public static ResultSet buildFromExportedKeys(List<ForeignKey> foreignKeys) throws SQLException {
        return buildFromForeignKeys(foreignKeys, "EXPORTED_KEYS");
    }

    public static ResultSet buildFromCrossReference(List<ForeignKey> foreignKeys) throws SQLException {
        return buildFromForeignKeys(foreignKeys, "CROSS_REFERENCE");
    }


    public static ResultSet buildFromForeignKeys(List<ForeignKey> foreignKeys, String entityName) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = foreignKeys.stream()
                .map(MetaResultSetBuilder::expandForeignKey)
                .flatMap(List::stream)
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                entityName,
                metaColumns,
                Arrays.asList(
                        new Parameter<>("PKTABLE_CAT", Types.VARCHAR, p -> p.getValue(0)),
                        new Parameter<>("PKTABLE_SCHEM", Types.VARCHAR, p -> p.getValue(1)),
                        new Parameter<>("PKTABLE_NAME", Types.VARCHAR, p -> p.getValue(2)),
                        new Parameter<>("PKCOLUMN_NAME", Types.VARCHAR, p -> p.getValue(3)),
                        new Parameter<>("FKTABLE_CAT", Types.VARCHAR, p -> p.getValue(4)),
                        new Parameter<>("FKTABLE_SCHEM", Types.VARCHAR, p -> p.getValue(5)),
                        new Parameter<>("FKTABLE_NAME", Types.VARCHAR, p -> p.getValue(6)),
                        new Parameter<>("FKCOLUMN_NAME", Types.VARCHAR, p -> p.getValue(7)),
                        new Parameter<>("KEY_SEQ", Types.SMALLINT, p -> p.getValue(8)),
                        new Parameter<>("UPDATE_RULE", Types.SMALLINT, p -> p.getValue(9)),
                        new Parameter<>("DELETE_RULE", Types.SMALLINT, p -> p.getValue(10)),
                        new Parameter<>("FK_NAME", Types.VARCHAR, p -> p.getValue(11)),
                        new Parameter<>("PK_NAME", Types.VARCHAR, p -> p.getValue(12)),
                        new Parameter<>("DEFERRABILITY", Types.SMALLINT, p -> null)
                )
        );
    }

    private static List<GenericMetaContainer> expandForeignKey(ForeignKey foreignKey) {
        AtomicInteger sequenceIndex = new AtomicInteger();
        List<Column> referencedKeyColumns = foreignKey.getReferencedColumnsList();
        return foreignKey.getForeignColumnsList().stream().map(c -> new GenericMetaContainer(
                foreignKey.getReferencedDatabaseName(),
                foreignKey.getReferencedNamespaceName(),
                foreignKey.getReferencedTableName(),
                referencedKeyColumns.get(sequenceIndex.get()).getDatabaseName(),
                c.getDatabaseName(),
                c.getNamespaceName(),
                c.getTableName(),
                c.getColumnName(),
                sequenceIndex.getAndIncrement(), // ATTENTION: inc sequence index after all accesses to the referenced key columns
                foreignKey.getUpdateRule(),
                foreignKey.getDeleteRule(),
                foreignKey.getKeyName()
        )).collect(Collectors.toList());
    }


    public static ResultSet buildFromTypes(List<Type> types) throws SQLException {
        return buildResultSet(
                "TYPE_INFO",
                types,
                Arrays.asList(
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, Type::getTypeName),
                        new Parameter<>("DATA_TYPE", Types.INTEGER, t -> TypedValueUtils.getJdbcTypeFromPolyTypeName(t.getTypeName())),
                        new Parameter<>("PRECISION", Types.INTEGER, Type::getPrecision),
                        new Parameter<>("LITERAL_PREFIX", Types.VARCHAR, nullIfFalse(Type::getLiteralPrefix, Type::hasLiteralPrefix)),
                        new Parameter<>("LITERAL_SUFFIX", Types.VARCHAR, nullIfFalse(Type::getLiteralSuffix, Type::hasLiteralSuffix)),
                        new Parameter<>("CREATE_PARAMS", Types.VARCHAR, p -> null),
                        new Parameter<>("NULLABLE", Types.SMALLINT, p -> DatabaseMetaData.typeNullable),
                        new Parameter<>("CASE_SENSITIVE", Types.BOOLEAN, Type::getIsCaseSensitive),
                        new Parameter<>("SEARCHABLE", Types.SMALLINT, integerAsShort(Type::getIsSearchable)),
                        new Parameter<>("UNSIGNED_ATTRIBUTE", Types.BOOLEAN, p -> false),
                        new Parameter<>("FIXED_PREC_SCALE", Types.BOOLEAN, p -> false),
                        new Parameter<>("AUTO_INCREMENT", Types.BOOLEAN, Type::getIsAutoIncrement),
                        new Parameter<>("LOCAL_TYPE_NAME", Types.VARCHAR, Type::getTypeName),
                        new Parameter<>("MINIMUM_SCALE", Types.SMALLINT, integerAsShort(Type::getMinScale)),
                        new Parameter<>("MAXIMUM_SCALE", Types.SMALLINT, integerAsShort(Type::getMaxScale)),
                        new Parameter<>("SQL_DATA_TYPE", Types.INTEGER, p -> null),
                        new Parameter<>("SQL_DATETIME_SUB", Types.INTEGER, p -> null),
                        new Parameter<>("NUM_PREC_RADIX", Types.INTEGER, Type::getRadix)
                )
        );
    }


    public static ResultSet buildFromIndexes(List<Index> indexes) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = indexes.stream()
                .map(MetaResultSetBuilder::expandIndex)
                .flatMap(List::stream)
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                "INDEX_INFO",
                metaColumns,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, p -> p.getValue(0)),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, p -> p.getValue(1)),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, p -> p.getValue(2)),
                        new Parameter<>("NON_UNIQUE", Types.BOOLEAN, p -> p.getValue(3)),
                        new Parameter<>("INDEX_QUALIFIER", Types.VARCHAR, p -> null),
                        new Parameter<>("INDEX_NAME", Types.VARCHAR, p -> p.getValue(4)),
                        new Parameter<>("TYPE", Types.TINYINT, p -> 0),
                        new Parameter<>("ORDINAL_POSITION", Types.TINYINT, integerAsShort(p -> p.getValue(5))),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, p -> p.getValue(6)),
                        new Parameter<>("ASC_OR_DESC", Types.VARCHAR, p -> null),
                        new Parameter<>("CARDINALITY", Types.BIGINT, p -> (long) -1),
                        new Parameter<>("PAGES", Types.BIGINT, p -> null),
                        new Parameter<>("FILTER_CONDITION", Types.VARCHAR, p -> null),
                        new Parameter<>("LOCATION", Types.VARCHAR, p -> p.getValue(7)),
                        new Parameter<>("INDEX_TYPE", Types.INTEGER, p -> p.getValue(8))
                )
        );
    }

    private static List<GenericMetaContainer> expandIndex(Index index) {
        AtomicInteger ordinalPosition = new AtomicInteger(1);
        return index.getColumnsList().stream().map(c -> new GenericMetaContainer(
                index.getDatabaseName(),
                index.getNamespaceName(),
                index.getTableName(),
                !index.getUnique(), // jdbc lists non uniqueness
                index.getIndexName(),
                ordinalPosition.getAndIncrement(),
                c.getColumnName(),
                index.getLocation(),
                index.getIndexType()
        )).collect(Collectors.toList());
    }

    public static ResultSet buildFromProcedures(List<Procedure> procedures) throws SQLException {
        // This creates an empty dummy result set because the requested information does not exist on the server side.
        return buildEmptyResultSet(
                "PROCEDURES",
                Arrays.asList(
                        new Parameter<>("PROCEDURE_CAT", Types.VARCHAR, p -> null),
                        new Parameter<>("PROCEDURE_SCHEM", Types.VARCHAR, p -> null),
                        new Parameter<>("PROCEDURE_NAME", Types.VARCHAR, Procedure::getTrivialName),
                        new Parameter<>("reserved for future use", Types.VARCHAR, p -> null),
                        new Parameter<>("reserved for future use", Types.VARCHAR, p -> null),
                        new Parameter<>("reserved for future use", Types.VARCHAR, p -> null),
                        new Parameter<>("REMARKS", Types.VARCHAR, Procedure::getDescription),
                        new Parameter<>("PROCEDURE_TYPE", Types.TINYINT, Procedure::getReturnTypeValue),
                        new Parameter<>("SPECIFIC_NAME", Types.VARCHAR, Procedure::getUniqueName)
                )
        );
    }

    public static ResultSet buildFromProcedureColumns() throws SQLException {
        return buildEmptyResultSet(
                "PROCEDURE_COLUMNS",
                Arrays.asList(
                        new Parameter<>("PROCEDURE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("PROCEDURE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("PROCEDURE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("COLUMN_TYPE", Types.TINYINT, DUMMY_ACCESSOR),
                        new Parameter<>("DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("PRECISION", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("LENGTH", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("SCALE", Types.TINYINT, DUMMY_ACCESSOR),
                        new Parameter<>("RADIX", Types.TINYINT, DUMMY_ACCESSOR),
                        new Parameter<>("NULLABLE", Types.TINYINT, DUMMY_ACCESSOR),
                        new Parameter<>("REMARKS", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("COLUMN_DEF", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SQL_DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("SQL_DATETIME_SUB", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("CHAR_OCTET_LENGTH", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("ORDINAL_POSITION", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SPECIFIC_NAME", Types.VARCHAR, DUMMY_ACCESSOR)
                )
        );
    }

    public static ResultSet buildFromColumnPrivileges(List<Column> columns, String userName) throws SQLException {
        List<GenericMetaContainer> columnPrivileges = columns.stream()
                .map(c -> createDummyColumnPrivileges(c, userName))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return buildResultSet(
                "COLUMN_PRIVELEGES",
                columnPrivileges,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, p -> p.getValue(0)),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, p -> p.getValue(1)),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, p -> p.getValue(2)),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, p -> p.getValue(3)),
                        new Parameter<>("GRANTOR", Types.TINYINT, p -> p.getValue(4)),
                        new Parameter<>("GRANTEE ", Types.INTEGER, p -> p.getValue(5)),
                        new Parameter<>("PRIVILEGE", Types.VARCHAR, p -> p.getValue(6)),
                        new Parameter<>("IS_GRANTABLE", Types.INTEGER, p -> p.getValue(7))
                )
        );
    }

    private static List<GenericMetaContainer> createDummyColumnPrivileges(Column colum, String userName) {
        // This method is used to create a dummy full rights result set for a column.
        List<String> accessRights = Arrays.asList("SELECT", "INSERT", "UPDATE", "REFERENCE");
        return accessRights.stream().map(a -> new GenericMetaContainer(
                colum.getDatabaseName(),
                colum.getNamespaceName(),
                colum.getTableName(),
                colum.getColumnName(),
                null,
                userName,
                a,
                "NO"
        )).collect(Collectors.toList());
    }

    public static <Tables> ResultSet buildFromTablePrivileges(List<Table> tables, String userName) throws SQLException {
        List<GenericMetaContainer> tablePrivileges = tables.stream()
                .map(t -> createDummyTablePrivileges(t, userName))
                .flatMap(List::stream)
                .collect(Collectors.toList());

        return buildResultSet(
                "TABLE_PRIVILEGES",
                tablePrivileges,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, p -> p.getValue(0)),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, p -> p.getValue(1)),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, p -> p.getValue(2)),
                        new Parameter<>("GRANTOR", Types.TINYINT, p -> p.getValue(3)),
                        new Parameter<>("GRANTEE ", Types.VARCHAR, p -> p.getValue(4)),
                        new Parameter<>("PRIVILEGE", Types.VARCHAR, p -> p.getValue(5)),
                        new Parameter<>("IS_GRANTABLE", Types.INTEGER, p -> p.getValue(6))
                )
        );
    }

    private static List<GenericMetaContainer> createDummyTablePrivileges(Table table, String userName) {
        // This method is used to create a dummy full rights result set for a table.
        List<String> accessRights = Arrays.asList("SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCE");
        return accessRights.stream().map(a -> new GenericMetaContainer(
                table.getSourceDatabaseName(),
                table.getNamespaceName(),
                table.getTableName(),
                null,
                userName,
                a,
                "NO"
        )).collect(Collectors.toList());
    }

    public static ResultSet buildFromVersionColumns(List<Column> columns) throws SQLException {
        return buildResultSet(
                "VERSION_COLUMNS",
                columns,
                Arrays.asList(
                        new Parameter<>("SCOPE", Types.TINYINT, p -> null),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, Column::getColumnName),
                        new Parameter<>("DATA_TYPE", Types.INTEGER, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName(p.getTypeName())),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, Column::getTypeName),
                        new Parameter<>("COLUMN_SIZE", Types.INTEGER, Column::getTypeLength),
                        new Parameter<>("BUFFER_LENGTH", Types.INTEGER, p -> null),
                        new Parameter<>("DECIMAL_DIGITS", Types.TINYINT, nullIfFalse(Column::getTypeScale, Column::hasTypeScale)),
                        new Parameter<>("PSEUDO_COLUMN", Types.TINYINT, p -> p.getIsHidden()
                                ? DatabaseMetaData.versionColumnPseudo
                                : DatabaseMetaData.versionColumnNotPseudo)
                )
        );
    }


    public static ResultSet buildFromSuperTypes() throws SQLException {
        return buildEmptyResultSet(
                "UDTs",
                Arrays.asList(
                        new Parameter<>("TYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SUPERTYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SUPERTYPE_SCHEM", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("SUPERTYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR)
                )
        );
    }

    public static ResultSet buildFromSuperTables() throws SQLException {
        return buildEmptyResultSet(
                "SUPERTABLES",
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SUPERTABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR)
                )
        );
    }

    public static ResultSet buildFromAttributes() throws SQLException {
        return buildEmptyResultSet(
                "ATTRIBUTES",
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("ATTR_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("ATTR_TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("ATTR_SIZE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("DECIMAL_DIGITS", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("NUM_PREC_RADIX", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("REMARKS", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("ATTR_DEF", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SQL_DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SQL_DATETIME_SUB", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("CHAR_OCTET_LENGTH", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("ORDINAL_POSITION", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SCOPE_CATALOG", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SCOPE_SCHEMA", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SCOPE_TABLE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SOURCE_DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR)
                )
        );
    }

    public static ResultSet buildFromClientInfoPropertyMetas(List<ClientInfoPropertyMeta> metas) throws SQLException {
        return buildResultSet(
                "CLIENT_INFO_PROPERTIES",
                metas,
                Arrays.asList(
                        new Parameter<>("NAME", Types.VARCHAR, ClientInfoPropertyMeta::getKey),
                        new Parameter<>("MAX_LEN", Types.VARCHAR, ClientInfoPropertyMeta::getMaxlength),
                        new Parameter<>("DEFAULT_VALUE", Types.VARCHAR, ClientInfoPropertyMeta::getDefaultValue),
                        new Parameter<>("DESCRIPTION", Types.VARCHAR, ClientInfoPropertyMeta::getDescription)
                )
        );
    }

    public static ResultSet buildFromPseudoColumns(List<Column> columns) throws SQLException {
        return buildResultSet(
                "PSEUDO_COLUMNS",
                columns,
                Arrays.asList(
                        new Parameter<>("TABLE_CAT", Types.VARCHAR, Column::getDatabaseName),
                        new Parameter<>("TABLE_SCHEM", Types.VARCHAR, Column::getNamespaceName),
                        new Parameter<>("TABLE_NAME", Types.VARCHAR, Column::getTableName),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, Column::getColumnName),
                        new Parameter<>("DATA_TYPE", Types.VARCHAR, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName(p.getTypeName())),
                        new Parameter<>("COLUMN_SIZE", Types.INTEGER, nullIfFalse(Column::getTypeLength, Column::hasTypeLength)),
                        new Parameter<>("DECIMAL_DIGITS", Types.INTEGER, nullIfFalse(Column::getTypeScale, Column::hasTypeScale)),
                        new Parameter<>("NUM_PREC_RADIX", Types.INTEGER, p -> null),
                        new Parameter<>("COLUMN_USAGE", Types.VARCHAR, p -> PseudoColumnUsage.USAGE_UNKNOWN),
                        new Parameter<>("REMARKS", Types.VARCHAR, p -> null),
                        new Parameter<>("CHAR_OCTET_LENGTH", Types.INTEGER, p -> null),
                        new Parameter<>("IS_NULLABLE", Types.VARCHAR, p -> p.getIsNullable() ? "YES" : "NO")
                )
        );
    }

    public static ResultSet fromBestRowIdentifiers(List<Column> columns) throws SQLException {
        return buildResultSet(
                "BEST_ROW_IDENTIFIERS",
                columns,
                Arrays.asList(
                        new Parameter<>("SCOPE", Types.VARCHAR, Column::getDatabaseName),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, Column::getColumnName),
                        new Parameter<>("DATA_TYPE", Types.VARCHAR, p -> TypedValueUtils.getJdbcTypeFromPolyTypeName(p.getTypeName())),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, Column::getTypeName),
                        new Parameter<>("COLUMN_SIZE", Types.INTEGER, nullIfFalse(Column::getTypeLength, Column::hasTypeLength)),
                        new Parameter<>("BUFFER_LENGTH", Types.INTEGER, p -> null),
                        new Parameter<>("DECIMAL_DIGITS", Types.SMALLINT, nullIfFalse(Column::getTypeScale, Column::hasTypeScale)),
                        new Parameter<>("PSEUDO_COLUMN", Types.SMALLINT, p -> p.getIsHidden()
                                ? DatabaseMetaData.bestRowPseudo
                                : DatabaseMetaData.bestRowNotPseudo
                        )
                )
        );
    }

    public static ResultSet buildFromUserDefinedTypes(List<UserDefinedType> userDefinedTypes) throws SQLException {
        return buildEmptyResultSet(
                "USER_DEFINED_TYPES",
                Arrays.asList(
                        new Parameter<>("TYPE_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("CLASS_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("DATA_TYPE", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("REMARKS", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("BASE_TYPE", Types.SMALLINT, DUMMY_ACCESSOR)
                )
        );
    }

    public static ResultSet fromFunctions(List<org.polypheny.jdbc.proto.Function> functions) throws SQLException {
        return buildResultSet(
                "FUNCTIONS",
                functions,
                Arrays.asList(
                        new Parameter<>("FUNCTION_CAT", Types.VARCHAR, p -> null),
                        new Parameter<>("FUNCTION_SCHEM", Types.VARCHAR, p -> null),
                        new Parameter<>("FUNCTION_NAME", Types.VARCHAR, org.polypheny.jdbc.proto.Function::getName),
                        new Parameter<>("REMARKS", Types.VARCHAR, org.polypheny.jdbc.proto.Function::getSyntax),
                        new Parameter<org.polypheny.jdbc.proto.Function>("FUNCTION_TYPE", Types.SMALLINT, p -> p.getIsTableFunction()
                                ? DatabaseMetaData.functionReturnsTable
                                : DatabaseMetaData.functionNoTable),
                        new Parameter<>("REMARKS", Types.VARCHAR, org.polypheny.jdbc.proto.Function::getName)
                )
        );

    }

    public static ResultSet buildFromFunctionColumns() throws SQLException {
        return buildEmptyResultSet(
                "FUNCTION_COLUMNS",
                Arrays.asList(
                        new Parameter<>("FUNCTION_CAT", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("FUNCTION_SCHEM", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("FUNCTION_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("COLUMN_NAME", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("COLUMN_TYPE", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("DATA_TYPE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("TYPE_NAME", Types.SMALLINT, DUMMY_ACCESSOR),
                        new Parameter<>("PRECISION", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("LENGTH", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SCALE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("RADIX", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("NULLABLE", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("REMARKS", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("CHAR_OCTET_LENGTH", Types.SMALLINT, DUMMY_ACCESSOR),
                        new Parameter<>("ORDINAL_POSITION", Types.INTEGER, DUMMY_ACCESSOR),
                        new Parameter<>("IS_NULLABLE", Types.VARCHAR, DUMMY_ACCESSOR),
                        new Parameter<>("SPECIFIC_NAME", Types.SMALLINT, DUMMY_ACCESSOR)
                )
        );
    }


    static class Parameter<T> {

        private final String name;
        private final int jdbcType;
        private final Function<T, Object> accessFunction;


        Parameter(String name, int jdbcType, Function<T, Object> acessor) {
            this.name = name;
            this.jdbcType = jdbcType;
            this.accessFunction = acessor;
        }


        TypedValue retrieveFrom(T message) throws SQLException {
            return TypedValue.fromObject(accessFunction.apply(message), jdbcType);
        }

    }

}



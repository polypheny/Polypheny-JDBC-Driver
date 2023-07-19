package org.polypheny.jdbc.meta;

import org.polypheny.jdbc.PolyphenyBidirectionalResultSet;
import org.polypheny.jdbc.proto.*;
import org.polypheny.jdbc.types.TypedValue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toCollection;

public class MetaResultSetBuilder {

    private static <T> PolyphenyBidirectionalResultSet buildEmptyResultSet(String entityName, List<MetaResultSetParameter<T>> metaResultSetParameters) throws SQLException {
        ArrayList<PolyphenyColumnMeta> columnMetas = buildMetas(entityName, metaResultSetParameters);
        ArrayList<ArrayList<TypedValue>> rows = new ArrayList<>();
        return new PolyphenyBidirectionalResultSet(columnMetas, rows);
    }

    private static <T> PolyphenyBidirectionalResultSet buildResultSet(String entityName, List<T> messages, List<MetaResultSetParameter<T>> metaResultSetParameters) throws SQLException {
        ArrayList<PolyphenyColumnMeta> columnMetas = buildMetas(entityName, metaResultSetParameters);
        ArrayList<ArrayList<TypedValue>> rows = buildRows(messages, metaResultSetParameters);
        return new PolyphenyBidirectionalResultSet(columnMetas, rows);
    }


    private static <T> ArrayList<PolyphenyColumnMeta> buildMetas(String entityName, List<MetaResultSetParameter<T>> metaResultSetParameters) {
        AtomicInteger ordinal = new AtomicInteger();
        return metaResultSetParameters.stream()
                .map(p -> PolyphenyColumnMeta.fromSpecification(ordinal.getAndIncrement(), p.getName(), entityName, p.getJdbcType()))
                .collect(toCollection(ArrayList::new));
    }


    private static <T> ArrayList<ArrayList<TypedValue>> buildRows(List<T> messages, List<MetaResultSetParameter<T>> metaResultSetParameters) throws SQLException {
        ArrayList<ArrayList<TypedValue>> arrayLists = new ArrayList<>();
        for (T p : messages) {
            ArrayList<TypedValue> typedValues = buildRow(p, metaResultSetParameters);
            arrayLists.add(typedValues);
        }
        return arrayLists;
    }


    private static <T> ArrayList<TypedValue> buildRow(T message, List<MetaResultSetParameter<T>> metaResultSetParameters) throws SQLException {
        ArrayList<TypedValue> typedValues = new ArrayList<>();
        for (MetaResultSetParameter<T> p : metaResultSetParameters) {
            TypedValue typedValue = p.retrieveFrom(message);
            typedValues.add(typedValue);
        }
        return typedValues;
    }


    public static ResultSet buildFromTables(List<Table> tables) throws SQLException {
        // jdbc standard about tables: Rows are ordered by TABLE_TYPE, TABLE_CAT, TABLE_SCHEM and TABLE_NAME ascending
        tables.sort(MetaResultSetComparators.TABLE_COMPARATOR);
        return buildResultSet(
                "TABLES",
                tables,
                MetaResultSetSignatures.TABLE_SIGNATURE
        );
    }


    public static ResultSet buildFromTableTypes(List<TableType> tableTypes) throws SQLException {
        return buildResultSet(
                "TABLE_TYPES",
                tableTypes,
                MetaResultSetSignatures.TABLE_TYPE_SIGNATURE
        );
    }


    public static ResultSet buildFromNamespaces(List<Namespace> namespaces) throws SQLException {
        // jdbc standard about schemas: Rows are ordered by TABLE_CATALOG and TABLE_SCHEM ascending
        namespaces.sort(MetaResultSetComparators.NAMESPACE_COMPARATOR);
        return buildResultSet(
                "NAMESPACES",
                namespaces,
                MetaResultSetSignatures.NAMESPACE_SIGNATURE
        );
    }


    public static ResultSet buildFromColumns(List<Column> columns) throws SQLException {
        // jdbc standard about columns: Rows are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, and ORDINAL_POSITION
        columns.sort(MetaResultSetComparators.COLUMN_COMPARATOR);
        return buildResultSet(
                "COLUMNS",
                columns,
                MetaResultSetSignatures.COLUMN_SIGNATURE
        );
    }


    public static ResultSet buildFromPrimaryKeys(List<PrimaryKey> primaryKeys) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = primaryKeys.stream()
                .map(MetaResultSetBuilder::expandPrimaryKey)
                .flatMap(List::stream)
                .sorted(MetaResultSetComparators.PRIMARY_KEY_COMPARATOR) // jdbc standard about primary keys: Rows are ordered by COLUMN_NAME ascending
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                "PRIMARY_KEYS",
                metaColumns,
                MetaResultSetSignatures.PRIMARY_KEY_GMC_SIGNATURE
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
                null
        )).collect(Collectors.toList());
    }


    public static ResultSet buildFromDatabases(List<Database> databases) throws SQLException {
        // jdbc standard about catalogs: Rows are ordered by TABLE_CAT ascending
        databases.sort(MetaResultSetComparators.DATABASE_COMPARATOR);
        return buildResultSet(
                "CATALOGS",
                databases,
                MetaResultSetSignatures.CATALOG_SIGNATURE
        );
    }

    public static ResultSet buildFromImportedKeys(List<ForeignKey> foreignKeys) throws SQLException {
        // jdbc standard about imported keys: Rows are ordered by PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, and KEY_SEQ ascending
        return buildFromForeignKeys(foreignKeys, "IMPORTED_KEYS", MetaResultSetComparators.IMPORTED_KEYS_COMPARATOR);
    }

    public static ResultSet buildFromExportedKeys(List<ForeignKey> foreignKeys) throws SQLException {
        // jdbc standard about exported keys: Rows are ordered by PKTABLE_NAME, PKCOLUMN_NAME, FKTABLE_NAME, and FKEY_SEQ ascending
        return buildFromForeignKeys(foreignKeys, "EXPORTED_KEYS", MetaResultSetComparators.EXPORTED_KEYS_COMPARATOR);
    }

    public static ResultSet buildFromCrossReference(List<ForeignKey> foreignKeys) throws SQLException {
        // jdbc standard about primary keys: Rows are ordered by COLUMN_NAME ascending
        return buildFromForeignKeys(foreignKeys, "CROSS_REFERENCE", MetaResultSetComparators.CROSS_REFERENCE_COMPARATOR);
    }


    private static ResultSet buildFromForeignKeys(List<ForeignKey> foreignKeys, String entityName, Comparator<GenericMetaContainer> comparator) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = foreignKeys.stream()
                .map(MetaResultSetBuilder::expandForeignKey)
                .flatMap(List::stream)
                .sorted(comparator)
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                entityName,
                metaColumns,
                MetaResultSetSignatures.FOREIGN_KEY_GMC_SIGNATURE
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
                sequenceIndex.getAndIncrement(), // ATTENTION: inc sequence index after all accesses to the referenced key columns so we access the proper columns (0-indexed) before and set the proper ordinal for jdbc (1-indexed)
                foreignKey.getUpdateRule(),
                foreignKey.getDeleteRule(),
                foreignKey.getKeyName()
        )).collect(Collectors.toList());
    }


    public static ResultSet buildFromTypes(List<Type> types) throws SQLException {
        // jdbc standard about type info: Rows are ordered by DATA_TYPE ascending
        types = types.stream().sorted(MetaResultSetComparators.TYPE_INFO_COMPARATOR).collect(Collectors.toList());
        return buildResultSet(
                "TYPE_INFO",
                types,
                MetaResultSetSignatures.TYPE_SIGNATURE
        );
    }


    public static ResultSet buildFromIndexes(List<Index> indexes) throws SQLException {
        ArrayList<GenericMetaContainer> metaColumns = indexes.stream()
                .map(MetaResultSetBuilder::expandIndex)
                .flatMap(List::stream)
                .sorted(MetaResultSetComparators.INDEX_COMPARATOR) // jdbc standard about indexes: Rows are ordered by NON_UNIQUE, INDEX_NAME, and ORDINAL_POSITION ascending
                .collect(toCollection(ArrayList::new));

        return buildResultSet(
                "INDEX_INFO",
                metaColumns,
                MetaResultSetSignatures.INDEX_GMC_SIGNATURE
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
                MetaResultSetSignatures.PROCEDURE_SIGNATURE
        );
    }

    public static ResultSet buildFromProcedureColumns() throws SQLException {
        return buildEmptyResultSet(
                "PROCEDURE_COLUMNS",
                MetaResultSetSignatures.PROCEDURE_COLUMN_EMPTY_SIGNATURE
        );
    }

    public static ResultSet buildFromColumnPrivileges(List<Column> columns, String userName) throws SQLException {
        List<GenericMetaContainer> columnPrivileges = columns.stream()
                .map(c -> createDummyColumnPrivileges(c, userName))
                .flatMap(List::stream)
                .sorted(MetaResultSetComparators.COLUMN_PRIVILEGE_COMPARATOR) //jdbc standard on column privileges: Rows are ordered by COLUMN_NAME and PRIVILEGE
                .collect(Collectors.toList());

        return buildResultSet(
                "COLUMN_PRIVELEGES",
                columnPrivileges,
                MetaResultSetSignatures.COLUMN_PRIVILEGES_GMC_SIGNATURE
        );
    }

    private static List<GenericMetaContainer> createDummyColumnPrivileges(Column colum, String userName) {
        // This method is used to create a dummy full rights result set for a column because the requested information does not exist on the server side.
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

    public static  ResultSet buildFromTablePrivileges(List<Table> tables, String userName) throws SQLException {
        List<GenericMetaContainer> tablePrivileges = tables.stream()
                .map(t -> createDummyTablePrivileges(t, userName))
                .flatMap(List::stream)
                .sorted(MetaResultSetComparators.TABLE_PRIVILEGE_COMPARATOR) //jdbc standard on column privileges: Rows are ordered by TABLE_CAT, TABLE_SCHEM, TABLE_NAME, and PRIVILEGE
                .collect(Collectors.toList());

        return buildResultSet(
                "TABLE_PRIVILEGES",
                tablePrivileges,
                MetaResultSetSignatures.TABLE_PRIVILEGES_GMC_SIGNATURE
        );
    }

    private static List<GenericMetaContainer> createDummyTablePrivileges(Table table, String userName) {
        // This method is used to create a dummy full rights result set for a table because the requested information does not exist on the server side.
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
                MetaResultSetSignatures.VERSION_COLUMN_SIGNATURE
        );
    }


    public static ResultSet buildFromSuperTypes() throws SQLException {
        return buildEmptyResultSet(
                "SUPER_TYPES",
                MetaResultSetSignatures.SUPER_TYPES_EMPTY_SIGNATURE
        );
    }

    public static ResultSet buildFromSuperTables() throws SQLException {
        return buildEmptyResultSet(
                "SUPER_TABLES",
                MetaResultSetSignatures.SUPER_TABLES_EMPTY_SIGNATURE
        );
    }

    public static ResultSet buildFromAttributes() throws SQLException {
        return buildEmptyResultSet(
                "ATTRIBUTES",
                MetaResultSetSignatures.ATTRIBUTES_EMPTY_SIGNATURE
        );
    }

    public static ResultSet buildFromClientInfoPropertyMetas(List<ClientInfoPropertyMeta> metas) throws SQLException {
        // jdbc standard about client info properties: Rows are ordered by NAME ascending
        metas.sort(MetaResultSetComparators.CLIENT_INFO_PROPERTY_COMPARATOR);
        return buildResultSet(
                "CLIENT_INFO_PROPERTIES",
                metas,
                MetaResultSetSignatures.CLIENT_INFO_PROPERTY_SIGNATURE
        );
    }

    public static ResultSet buildFromPseudoColumns(List<Column> columns) throws SQLException {
        // jdbc standard about pseudo columns: Rows are ordered by TABLE_CAT,TABLE_SCHEM, TABLE_NAME and COLUMN_NAME.
        columns.sort(MetaResultSetComparators.PSEUDO_COLUMN_COMPARATOR);
        return buildResultSet(
                "PSEUDO_COLUMNS",
                columns,
                MetaResultSetSignatures.PSEUDO_COLUMN_SIGNATURE
        );
    }

    public static ResultSet fromBestRowIdentifiers(List<Column> columns) throws SQLException {
       // sorting can be ignored as the key is not supported by polypheny and thus set to a constant manually in the client
        return buildResultSet(
                "BEST_ROW_IDENTIFIERS",
                columns,
                MetaResultSetSignatures.BEST_ROW_IDENTIFIER_SIGNATURE
        );
    }

    public static ResultSet buildFromUserDefinedTypes(List<UserDefinedType> userDefinedTypes) throws SQLException {
        return buildEmptyResultSet(
                "USER_DEFINED_TYPES",
                MetaResultSetSignatures.USER_DEFINED_TYPE_EMPTY_SIGNATURE
        );
    }

    public static ResultSet fromFunctions(List<org.polypheny.jdbc.proto.Function> functions) throws SQLException {
        // jdbc standard about functions: Rows are ordered by FUNCTION_CAT, FUNCTION_SCHEM, FUNCTION_NAME and SPECIFIC_NAME ascending
        functions = functions.stream().sorted(MetaResultSetComparators.FUNCTION_COMPARATOR).collect(Collectors.toList());
        return buildResultSet(
                "FUNCTIONS",
                functions,
                MetaResultSetSignatures.FUNCTION_SIGNATURE
        );

    }

    public static ResultSet buildFromFunctionColumns() throws SQLException {
        return buildEmptyResultSet(
                "FUNCTION_COLUMNS",
                MetaResultSetSignatures.FUNCTION_COLUMN_EMPTY_SIGNATURE
        );
    }
}



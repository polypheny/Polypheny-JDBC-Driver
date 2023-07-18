package org.polypheny.jdbc.meta;

import io.grpc.StatusRuntimeException;
import org.polypheny.jdbc.ConnectionString;
import org.polypheny.jdbc.PolyphenyConnection;
import org.polypheny.jdbc.ProtoInterfaceClient;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.proto.*;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PolyphenyDatabaseMetadata implements DatabaseMetaData {

    private static final int NO_VERSION = -1;
    private ConnectionString connectionString;

    private NullSorting nullSorting;

    private ProtoInterfaceClient protoInterfaceClient;

    private PolyphenyConnection polyphenyConnection;

    private String productName;
    private String productVersion;
    private int databaseMinorVersion = NO_VERSION;
    private int databaseMajorVersion = NO_VERSION;


    // TODO TH: remove and hardcode this
    private enum NullSorting {
        START,
        END,
        HIGH,
        LOW
    }


    public PolyphenyDatabaseMetadata(ProtoInterfaceClient protoInterfaceClient, ConnectionString target) {
        this.protoInterfaceClient = protoInterfaceClient;
        this.connectionString = target;
        //TODO TH: check what polypheny does...
        this.nullSorting = NullSorting.HIGH;
    }

    private void throwNotSupportedIfStrict() throws SQLFeatureNotSupportedException {
        if (!DriverProperties.isSTRICT_MODE()) {
            return;
        }
        throw new SQLFeatureNotSupportedException();
    }


    public void setConnection(PolyphenyConnection connection) {
        this.polyphenyConnection = connection;
    }


    private void fetchDbmsVersionInfo() {
        DbmsVersionResponse response = protoInterfaceClient.getDbmsVersion();
        productName = response.getDbmsName();
        productVersion = response.getVersionName();
        databaseMinorVersion = response.getMinorVersion();
        databaseMajorVersion = response.getMajorVersion();
    }


    @Override
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }


    @Override
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }


    @Override
    public String getURL() throws SQLException {
        if (connectionString == null) {
            return null;
        }
        return DriverProperties.getDRIVER_URL_SCHEMA() + "//" + connectionString.getTarget();
    }


    @Override
    public String getUserName() throws SQLException {
        if (connectionString == null) {
            return null;
        }
        return connectionString.getUser();
    }


    @Override
    public boolean isReadOnly() throws SQLException {
        return PropertyUtils.isDEFAULT_AUTOCOMMIT();
    }


    @Override
    public boolean nullsAreSortedHigh() throws SQLException {
        return nullSorting == NullSorting.HIGH;
    }


    @Override
    public boolean nullsAreSortedLow() throws SQLException {
        return nullSorting == NullSorting.LOW;
    }


    @Override
    public boolean nullsAreSortedAtStart() throws SQLException {
        return nullSorting == NullSorting.START;
    }


    @Override
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return nullSorting == NullSorting.END;
    }


    @Override
    public String getDatabaseProductName() throws SQLException {
        if (productName == null) {
            fetchDbmsVersionInfo();
        }
        return productName;
    }


    @Override
    public String getDatabaseProductVersion() throws SQLException {
        if (productVersion == null) {
            fetchDbmsVersionInfo();
        }
        return productVersion;
    }


    @Override
    public String getDriverName() throws SQLException {
        return DriverProperties.getDRIVER_NAME();
    }


    @Override
    public String getDriverVersion() throws SQLException {
        return DriverProperties.getDRIVER_VERSION();
    }


    @Override
    public int getDriverMajorVersion() {
        return DriverProperties.getDRIVER_MAJOR_VERSION();
    }


    @Override
    public int getDriverMinorVersion() {
        return DriverProperties.getDRIVER_MINOR_VERSION();
    }


    @Override
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }


    @Override
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return true;
    }


    @Override
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return false;
    }


    @Override
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }


    @Override
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }


    @Override
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }


    @Override
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }


    @Override
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }


    @Override
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }


    @Override
    public String getSQLKeywords() throws SQLException {
        return protoInterfaceClient.getSqlKeywords();
    }


    @Override
    public String getNumericFunctions() throws SQLException {
        return protoInterfaceClient.getSqlNumericFunctions();
    }


    @Override
    public String getStringFunctions() throws SQLException {
        return protoInterfaceClient.getSqlStringFunctions();
    }


    @Override
    public String getSystemFunctions() throws SQLException {
        return protoInterfaceClient.getSqlSystemFunctions();
    }


    @Override
    public String getTimeDateFunctions() throws SQLException {
        return protoInterfaceClient.getSqlTimeDateFunctions();
    }


    @Override
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }


    @Override
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }


    @Override
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }


    @Override
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsConvert() throws SQLException {
        //TODO TH: change upon implementation
        return false;
    }


    @Override
    public boolean supportsConvert(int fromType, int toType) throws SQLException {
        return false;
    }


    @Override
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsMultipleResultSets() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsMultipleTransactions() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }


    @Override
    public String getSchemaTerm() throws SQLException {
        return "namespace";
    }


    @Override
    public String getProcedureTerm() throws SQLException {
        // Stored procedures not supported...
        return "procedure";
    }


    @Override
    public String getCatalogTerm() throws SQLException {
        return "database";
    }


    @Override
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }


    @Override
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }


    @Override
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        // Index definition not supported by polypheny!
        return true;
    }


    @Override
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        // Privilege definition not supported by polypheny!
        return true;
    }


    @Override
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        // Index definition not supported by polypheny!
        return true;
    }


    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        // Privilege Definition not supported by polypheny!
        return true;
    }


    @Override
    public boolean supportsPositionedDelete() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsPositionedUpdate() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsSelectForUpdate() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsStoredProcedures() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsUnion() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return false;
    }


    @Override
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxConnections() throws SQLException {
        // GRPC supports 100 concurrent streams by default. Beyond this queuing occurs.
        return 0;
    }


    @Override
    public int getMaxCursorNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxSchemaNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxProcedureNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxCatalogNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxRowSize() throws SQLException {
        return 0;
    }


    @Override
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return false;
    }


    @Override
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxStatements() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxTableNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }


    @Override
    public int getMaxUserNameLength() throws SQLException {
        return 0;
    }


    @Override
    public int getDefaultTransactionIsolation() throws SQLException {
        return PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION();
    }


    @Override
    public boolean supportsTransactions() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsTransactionIsolationLevel(int level) throws SQLException {
        // This is the only supported isolation level
        return level == PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION();
    }


    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }


    @Override
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return false;
    }


    @Override
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }


    @Override
    public ResultSet getProcedures(String catalog, String schemaPattern, String procedureNamePattern) throws SQLException {
        throwNotSupportedIfStrict();
        List<Procedure> procedures = protoInterfaceClient.searchProcedures("sql", procedureNamePattern);
        return MetaResultSetBuilder.buildFromProcedures(procedures);
    }


    @Override
    public ResultSet getProcedureColumns(String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern) throws SQLException {
        // For now an empty result set is returned
        // For the production version the getProcedures api call should be used to retrieve the procedures meta
        // which will contain all info required to build this result set.
        throwNotSupportedIfStrict();
        return MetaResultSetBuilder.buildFromProcedureColumns();
    }


    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        // catalog ignored because polypheny doesn't have those
        try {
            List<Table> tables = getTableStream(schemaPattern, tableNamePattern).collect(Collectors.toList());
            if (types == null) {
                return MetaResultSetBuilder.buildFromTables(tables);
            }
            HashSet<String> tableTypes = new HashSet<>(Arrays.asList(types));
            tables = tables.stream().filter(t -> tableTypes.contains(t.getTableType())).collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromTables(tables);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public ResultSet getSchemas() throws SQLException {
        try {
            return getSchemas(null, null);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getCatalogs() throws SQLException {
        try {
            List<Database> databases = protoInterfaceClient.getDatabases();
            return MetaResultSetBuilder.buildFromDatabases(databases);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getTableTypes() throws SQLException {
        try {
            List<TableType> tableTypes = protoInterfaceClient.getTablesTypes();
            return MetaResultSetBuilder.buildFromTableTypes(tableTypes);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try {
            List<Column> columns = protoInterfaceClient.searchNamespaces(schemaPattern, MetaUtils.NamespaceTypes.RELATIONAL.name())
                    .stream()
                    .map(n -> getMatchingColumns(n, tableNamePattern, columnNamePattern))
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromColumns(columns);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }

    private List<Column> getMatchingColumns(Namespace namespace, String tableNamePattern, String columnNamePattern) {
        Stream<Column> columnStream = protoInterfaceClient.searchEntities(namespace.getNamespaceName(), tableNamePattern).stream()
                .filter(Entity::hasTable)
                .map(Entity::getTable)
                .map(Table::getColumnsList)
                .flatMap(List::stream);
        if (columnNamePattern == null) {
            return columnStream.collect(Collectors.toList());
        }
        return columnStream
                .filter(c -> columnMatchesPattern(namespace, c, columnNamePattern))
                .collect(Collectors.toList());
    }

    private boolean columnMatchesPattern (Namespace namespace, Column column, String columnNamePattern) {
        if (namespace.getIsCaseSensitive()) {
            return column.getColumnName().matches(MetaUtils.convertToRegex(columnNamePattern));
        }
        return column.getColumnName().toLowerCase().matches(MetaUtils.convertToRegex(columnNamePattern.toLowerCase()));
    }

    @Override
    public ResultSet getColumnPrivileges(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        /* This feature is currently not supported by polypheny thus the following workaround is used:
         * 1) get all columns using getColumns()
         * 2) the MetaResultSetBuilder constructs a full rights result set from the response of the getCoumns() api call.
         *
         * For proper implementation a dedicated api call should be used the result of witch should be passed to the MetaResultSet builder.
         */
        throwNotSupportedIfStrict();
        try {
            List<Column> columns = protoInterfaceClient.searchNamespaces(schemaPattern, MetaUtils.NamespaceTypes.RELATIONAL.name())
                    .stream()
                    .map(n -> getMatchingColumns(n, tableNamePattern, columnNamePattern))
                    .flatMap(List::stream)
                    .collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromColumnPrivileges(columns, getUserName());
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }




    @Override
    public ResultSet getTablePrivileges(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        /* This feature is currently not supported by polypheny thus the following workaround is used:
         * 1) get all tables using getColumns()
         * 2) the MetaResultSetBuilder constructs a full rights result set from the response of the searchNamespaces() and searchEntities() api calls.
         */
        throwNotSupportedIfStrict();
        try {
            List<Table> tables = getTableStream(schemaPattern, tableNamePattern).collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromTablePrivileges(tables, getUserName());
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getBestRowIdentifier(String catalog, String schema, String table, int scope, boolean nullable) throws SQLException {
        List<Column> columns = getTableStream(schema, table)
                .filter(Table::hasPrimaryKey)
                .map(Table::getPrimaryKey)
                .map(PrimaryKey::getColumnsList)
                .flatMap(List::stream)
                .filter(c -> !c.getIsNullable() || nullable)
                .collect(Collectors.toList());
        return MetaResultSetBuilder.fromBestRowIdentifiers(columns);
    }


    @Override
    public ResultSet getVersionColumns(String catalog, String schema, String table) throws SQLException {
        try {
            List<Column> columns = getTableStream(schema, table)
                    .map(Table::getColumnsList)
                    .flatMap(List::stream)
                    .filter(c -> c.getColumnType() == Column.ColumnType.VERSION)
                    .collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromVersionColumns(columns);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public ResultSet getPrimaryKeys(String catalog, String schema, String table) throws SQLException {
        List<PrimaryKey> primaryKeys = getTableStream(schema, table)
                .filter(Table::hasPrimaryKey)
                .map(Table::getPrimaryKey)
                .collect(Collectors.toList());
        return MetaResultSetBuilder.buildFromPrimaryKeys(primaryKeys);
    }


    @Override
    public ResultSet getImportedKeys(String catalog, String schema, String table) throws SQLException {
        List<ForeignKey> foreignKeys = getTableStream(schema, table)
                .map(Table::getForeignKeysList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        return MetaResultSetBuilder.buildFromImportedKeys(foreignKeys);
    }


    @Override
    public ResultSet getExportedKeys(String catalog, String schema, String table) throws SQLException {
        List<ForeignKey> exportedKeys = getTableStream(schema, table)
                .map(Table::getExportedKeysList)
                .flatMap(List::stream)
                .collect(Collectors.toList());
        return MetaResultSetBuilder.buildFromExportedKeys(exportedKeys);
    }

    private Stream<Table> getTableStream(String namespace, String table) {
        return protoInterfaceClient.searchNamespaces(namespace, MetaUtils.NamespaceTypes.RELATIONAL.name())
                .stream()
                .map(Namespace::getNamespaceName)
                .map(name -> protoInterfaceClient.searchEntities(name, table))
                .flatMap(List::stream)
                .filter(Entity::hasTable)
                .map(Entity::getTable);
    }


    @Override
    public ResultSet getCrossReference(String parentCatalog, String parentSchema, String parentTable,
                                       String foreignCatalog, String foreignSchema, String foreignTable) throws SQLException {
        HashMap<String, Table> parentTables = getTableStream(parentSchema, parentTable)
                .collect(Collectors.toMap(Table::getTableName, t -> t, (prev, next) -> next, HashMap::new));
        List<ForeignKey> foreignKeys = getTableStream(foreignSchema, foreignTable)
                .map(Table::getForeignKeysList)
                .flatMap(List::stream)
                .filter(f -> referencesTable(f, parentTables.get(f.getReferencedTableName())))
                .collect(Collectors.toList());
        return MetaResultSetBuilder.buildFromCrossReference(foreignKeys);
    }

    private boolean referencesTable(ForeignKey foreignKey, Table table) {
        if (table == null) {
            return false;
        }
        if (!foreignKey.getReferencedTableName().equals(table.getTableName())){
            return false;
        }
        if (!foreignKey.getReferencedNamespaceName().equals(table.getNamespaceName())){
            return false;
        }
        if (!foreignKey.getReferencedDatabaseName().equals(table.getSourceDatabaseName())){
            return false;
        }
        return true;
    }


    @Override
    public ResultSet getTypeInfo() throws SQLException {
        List<Type> types = protoInterfaceClient.getTypes();
        return MetaResultSetBuilder.buildFromTypes(types);
    }


    @Override
    public ResultSet getIndexInfo(String catalog, String schema, String table, boolean unique, boolean approximate) throws SQLException {
        List<Index> indexes = getTableStream(schema, table)
                .map(Table::getIndexesList)
                .flatMap(List::stream)
                .filter(i -> i.getUnique() || !unique)
                .collect(Collectors.toList());
        return MetaResultSetBuilder.buildFromIndexes(indexes);
    }


    @Override
    public boolean supportsResultSetType(int type) throws SQLException {
        return PropertyUtils.isValidResultSetType(type);
    }


    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency) throws SQLException {
        return PropertyUtils.isValidResultSetConcurrency(type, concurrency);
    }


    @Override
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean ownDeletesAreVisible(int tyoe) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }


    @Override
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }


    @Override
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }


    @Override
    public boolean updatesAreDetected(int i) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean deletesAreDetected(int i) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean insertsAreDetected(int i) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }


    @Override
    public ResultSet getUDTs(String catalog, String schemaPattern, String typeNamePattern, int[] types) throws SQLException {
        throwNotSupportedIfStrict();
        List<UserDefinedType> userDefinedTypes = protoInterfaceClient.getUserDefinedTypes();
        return MetaResultSetBuilder.buildFromUserDefinedTypes(userDefinedTypes);
    }


    @Override
    public Connection getConnection() throws SQLException {
        return polyphenyConnection;
    }


    @Override
    public boolean supportsSavepoints() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsMultipleOpenResults() throws SQLException {
        return false;
    }


    @Override
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return false;
    }


    @Override
    public ResultSet getSuperTypes(String catalog, String schemaPattern, String typeNamePattern) throws SQLException {
        throwNotSupportedIfStrict();
        return MetaResultSetBuilder.buildFromSuperTypes();
    }


    @Override
    public ResultSet getSuperTables(String catalog, String schemaPattern, String tableNamePattern) throws SQLException {
        throwNotSupportedIfStrict();
        return MetaResultSetBuilder.buildFromSuperTables();
    }


    @Override
    public ResultSet getAttributes(String catalog, String schemaPattern, String typeNamePattern, String attributeNamePattern) throws SQLException {
        throwNotSupportedIfStrict();
        // Now creates an empty result set. In the future the getUserDefinedTypes api call should be used to retrieve
        // the UDT meta which will contain all data necessary to build this result set.
        return MetaResultSetBuilder.buildFromAttributes();
    }


    @Override
    public boolean supportsResultSetHoldability(int resultSetHoldability) throws SQLException {
        return PropertyUtils.isValidResultSetHoldability(resultSetHoldability);
    }


    @Override
    public int getResultSetHoldability() throws SQLException {
        return PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY();
    }


    @Override
    public int getDatabaseMajorVersion() throws SQLException {
        if (databaseMajorVersion == NO_VERSION) {
            fetchDbmsVersionInfo();
        }
        return databaseMinorVersion;
    }


    @Override
    public int getDatabaseMinorVersion() throws SQLException {
        if (databaseMajorVersion == NO_VERSION) {
            fetchDbmsVersionInfo();
        }
        return databaseMinorVersion;
    }


    @Override
    public int getJDBCMajorVersion() throws SQLException {
        return DriverProperties.getDRIVER_MAJOR_VERSION();
    }


    @Override
    public int getJDBCMinorVersion() throws SQLException {
        return DriverProperties.getDRIVER_MINOR_VERSION();
    }


    @Override
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL;
    }


    @Override
    public boolean locatorsUpdateCopy() throws SQLException {
        return true;
    }


    @Override
    public boolean supportsStatementPooling() throws SQLException {
        return false;
    }


    @Override
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }


    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern) throws SQLException {
        List<Namespace> namespaces = protoInterfaceClient.searchNamespaces(schemaPattern, MetaUtils.NamespaceTypes.RELATIONAL.name());
        return MetaResultSetBuilder.buildFromNamespaces(namespaces);
    }


    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return false;
    }


    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }


    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        List<ClientInfoPropertyMeta> metas = protoInterfaceClient.getClientInfoPropertyMetas();
        return MetaResultSetBuilder.buildFromClientInfoPropertyMetas(metas);
    }


    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern) throws SQLException {
        List<Function> functions = protoInterfaceClient.searchFunctions("sql", "SYSTEM")
                .stream()
                .filter(f -> f.getName().matches(MetaUtils.convertToRegex(functionNamePattern)))
                .collect(Collectors.toList());
        return MetaResultSetBuilder.fromFunctions(functions);
    }


    @Override
    public ResultSet getFunctionColumns(String s, String s1, String s2, String s3) throws SQLException {
        throwNotSupportedIfStrict();
        return MetaResultSetBuilder.buildFromFunctionColumns();
    }


    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern) throws SQLException {
        try {
            List<Column> columns = protoInterfaceClient.searchNamespaces(schemaPattern, MetaUtils.NamespaceTypes.RELATIONAL.name())
                    .stream()
                    .map(n -> getMatchingColumns(n, tableNamePattern, columnNamePattern))
                    .flatMap(List::stream)
                    .filter(Column::getIsHidden)
                    .collect(Collectors.toList());
            return MetaResultSetBuilder.buildFromPseudoColumns(columns);
        } catch (StatusRuntimeException e) {
            throw new SQLException(e.getMessage());
        }
    }


    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
    }


    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new SQLException("Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return aClass.isInstance(this);

    }

}
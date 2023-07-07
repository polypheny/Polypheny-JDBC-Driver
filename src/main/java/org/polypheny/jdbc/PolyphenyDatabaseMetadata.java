package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import org.polypheny.jdbc.proto.DbmsVersionResponse;
import org.polypheny.jdbc.proto.TableTypesResponse;
import org.polypheny.jdbc.proto.TablesResponse;
import org.polypheny.jdbc.utils.MetaResultSetBuilder;
import org.polypheny.jdbc.utils.PropertyUtils;

public class PolyphenyDatabaseMetadata implements DatabaseMetaData {
    private static final int NO_VERSION = -1;
    private ConnectionString connectionString;

    private NullSorting nullSorting;

    private ProtoInterfaceClient protoInterfaceClient;

    private PolyphenyConnection polyphenyConnection;

    private String productName;
    private String productVersion;
    private int databaseMinorVersion= NO_VERSION;
    private int databaseMajorVersion = NO_VERSION;


    // TODO TH: remove and hardcode this
    private enum NullSorting {
        START,
        END,
        HIGH,
        LOW
    }


    public PolyphenyDatabaseMetadata( ProtoInterfaceClient protoInterfaceClient, ConnectionString target ) {
        this.protoInterfaceClient = protoInterfaceClient;
        this.connectionString = target;
        //TODO TH: check what polypheny does...
        this.nullSorting = NullSorting.HIGH;
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
        if ( connectionString == null ) {
            return null;
        }
        return DriverProperties.getDRIVER_URL_SCHEMA() + "//" + connectionString.getTarget();
    }


    @Override
    public String getUserName() throws SQLException {
        if ( connectionString == null ) {
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
        if ( productName == null ) {
            fetchDbmsVersionInfo();
        }
        return productName;
    }


    @Override
    public String getDatabaseProductVersion() throws SQLException {
        if ( productVersion == null ) {
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
        try {
            throw new NotImplementedException( "Feature not yet implemented" );
        } catch ( NotImplementedException e ) {
            throw new RuntimeException( e );
        }
        // TODO TH: implement this correctly
        // Comma-separated list of all of this database's SQL keywords that are NOT also SQL:2003 keywords.
    }


    @Override
    public String getNumericFunctions() throws SQLException {
        try {
            throw new NotImplementedException( "Feature not yet implemented" );
        } catch ( NotImplementedException e ) {
            throw new RuntimeException( e );
        }
        // TODO TH: implement this correctly
        // Retrieves a comma-separated list of math functions available with this database. These are the Open /Open CLI math function names used in the JDBC function escape clause.
    }


    @Override
    public String getStringFunctions() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getSystemFunctions() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getTimeDateFunctions() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getSearchStringEscape() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getExtraNameCharacters() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
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
    public boolean supportsConvert( int fromType, int toType ) throws SQLException {
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
    public boolean supportsTransactionIsolationLevel( int level ) throws SQLException {
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
    public ResultSet getProcedures( String catalog, String schemaPattern, String procedureNamePattern ) throws SQLException {
        // Stored procedures not supported.
        // TODO TH: implement empty result set
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getProcedureColumns( String catalog, String schemaPattern, String procedureNamePattern, String columnNamePattern ) throws SQLException {
        // Stored procedures not supported.
        // TODO TH: implement empty result set
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getTables( String catalog, String schemaPattern, String tableNamePattern, String[] types) throws SQLException {
        // we ignore the catalogs as polypheny does not have those.
        TablesResponse tablesResponse = protoInterfaceClient.getTables(schemaPattern, tableNamePattern, types);
        return MetaResultSetBuilder.buildFromTablesResponse( tablesResponse );
    }


    @Override
    public ResultSet getSchemas() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getCatalogs() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getTableTypes() throws SQLException {
        TableTypesResponse tableTypesResponse = protoInterfaceClient.getTablesTypes();
        return MetaResultSetBuilder.buildFromTableTypesResponse(tableTypesResponse);
    }


    @Override
    public ResultSet getColumns( String s, String s1, String s2, String s3 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getColumnPrivileges( String s, String s1, String s2, String s3 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getTablePrivileges( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getBestRowIdentifier( String s, String s1, String s2, int i, boolean b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getVersionColumns( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getPrimaryKeys( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getImportedKeys( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getExportedKeys( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getCrossReference( String s, String s1, String s2, String s3, String s4, String s5 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getTypeInfo() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getIndexInfo( String s, String s1, String s2, boolean b, boolean b1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean supportsResultSetType( int type ) throws SQLException {
        return PropertyUtils.isValidResultSetType( type );
    }


    @Override
    public boolean supportsResultSetConcurrency( int type, int concurrency ) throws SQLException {
        return PropertyUtils.isValidResultSetConcurrency( type, concurrency );
    }


    @Override
    public boolean ownUpdatesAreVisible( int type ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean ownDeletesAreVisible( int tyoe ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean ownInsertsAreVisible( int type ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean othersUpdatesAreVisible( int type ) throws SQLException {
        return false;
    }


    @Override
    public boolean othersDeletesAreVisible( int type ) throws SQLException {
        return false;
    }


    @Override
    public boolean othersInsertsAreVisible( int type ) throws SQLException {
        return false;
    }


    @Override
    public boolean updatesAreDetected( int i ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean deletesAreDetected( int i ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean insertsAreDetected( int i ) throws SQLException {
        //TODO TH: adjust according to implementation
        return false;
    }


    @Override
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }


    @Override
    public ResultSet getUDTs( String s, String s1, String s2, int[] ints ) throws SQLException {
        //TODO TH: implement this...
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
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
    public ResultSet getSuperTypes( String s, String s1, String s2 ) throws SQLException {
        //TODO TH: implement this...
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getSuperTables( String s, String s1, String s2 ) throws SQLException {
        //TODO TH: implement this...
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getAttributes( String s, String s1, String s2, String s3 ) throws SQLException {
        //TODO TH: implement this...
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean supportsResultSetHoldability( int resultSetHoldability ) throws SQLException {
        return PropertyUtils.isValidResultSetHoldability( resultSetHoldability );
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
    public ResultSet getSchemas( String s, String s1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getClientInfoProperties() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getFunctions( String s, String s1, String s2 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getFunctionColumns( String s, String s1, String s2, String s3 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSet getPseudoColumns( String s, String s1, String s2, String s3 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return false;
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

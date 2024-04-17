package org.polypheny.jdbc.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.ConnectionString;
import org.polypheny.jdbc.PolyphenyStatement;
import org.polypheny.jdbc.PrismInterfaceClient;

public class PolyConnectionPropertiesTest {

    private static ConnectionString connectionString;


    @BeforeAll
    public static void setUpClass() throws SQLException {
        connectionString = new ConnectionString( "jdbc:polypheny://localhost:20590" );
    }


    private static final PrismInterfaceClient PRISM_INTERFACE_CLIENT = mock( PrismInterfaceClient.class );
    private static final PolyphenyStatement polyphenyStatement = mock( PolyphenyStatement.class );


    @Test
    public void toStatementProperties_Type_Concurrency_Holdability_Conversion() throws SQLException {
        when( polyphenyStatement.hasStatementId() ).thenReturn( false );
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement( polyphenyStatement );
        expectedProperties.setPrismInterfaceClient( PRISM_INTERFACE_CLIENT );
        expectedProperties.setQueryTimeoutSeconds( PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS() );
        expectedProperties.setResultSetType( ResultSet.TYPE_SCROLL_INSENSITIVE );
        expectedProperties.setResultSetConcurrency( ResultSet.CONCUR_READ_ONLY );
        expectedProperties.setResultSetHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        expectedProperties.setFetchSize( PropertyUtils.getDEFAULT_FETCH_SIZE() );
        expectedProperties.setFetchDirection( PropertyUtils.getDEFAULT_FETCH_DIRECTION() );
        expectedProperties.setMaxFieldSize( PropertyUtils.getDEFAULT_MAX_FIELD_SIZE() );
        expectedProperties.setLargeMaxRows( PropertyUtils.getDEFAULT_LARGE_MAX_ROWS() );
        expectedProperties.setDoesEscapeProcessing( PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING() );
        expectedProperties.setIsPoolable( PropertyUtils.isDEFAULT_STATEMENT_POOLABLE() );

        PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
        );
        actualProperties.setPolyphenyStatement( polyphenyStatement );
        assertEquals( expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds() );
        assertEquals( expectedProperties.getResultSetType(), actualProperties.getResultSetType() );
        assertEquals( expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency() );
        assertEquals( expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability() );
        assertEquals( expectedProperties.getFetchSize(), actualProperties.getFetchSize() );
        assertEquals( expectedProperties.getFetchDirection(), actualProperties.getFetchDirection() );
        assertEquals( expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize() );
        assertEquals( expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows() );
        assertEquals( expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing() );
        assertEquals( expectedProperties.isPoolable(), actualProperties.isPoolable() );
    }


    @Test
    public void toStatementProperties_Type_Concurrency_Conversion() throws SQLException {
        when( polyphenyStatement.hasStatementId() ).thenReturn( false );
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement( polyphenyStatement );
        expectedProperties.setPrismInterfaceClient( PRISM_INTERFACE_CLIENT );
        expectedProperties.setQueryTimeoutSeconds( PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS() );
        expectedProperties.setResultSetType( ResultSet.TYPE_FORWARD_ONLY );
        expectedProperties.setResultSetConcurrency( ResultSet.CONCUR_READ_ONLY );
        expectedProperties.setResultSetHoldability( PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY() );
        expectedProperties.setFetchSize( PropertyUtils.getDEFAULT_FETCH_SIZE() );
        expectedProperties.setFetchDirection( PropertyUtils.getDEFAULT_FETCH_DIRECTION() );
        expectedProperties.setMaxFieldSize( PropertyUtils.getDEFAULT_MAX_FIELD_SIZE() );
        expectedProperties.setLargeMaxRows( PropertyUtils.getDEFAULT_LARGE_MAX_ROWS() );
        expectedProperties.setDoesEscapeProcessing( PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING() );
        expectedProperties.setIsPoolable( PropertyUtils.isDEFAULT_STATEMENT_POOLABLE() );

        PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties( ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY );
        actualProperties.setPolyphenyStatement( polyphenyStatement );
        assertEquals( expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds() );
        assertEquals( expectedProperties.getResultSetType(), actualProperties.getResultSetType() );
        assertEquals( expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency() );
        assertEquals( expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability() );
        assertEquals( expectedProperties.getFetchSize(), actualProperties.getFetchSize() );
        assertEquals( expectedProperties.getFetchDirection(), actualProperties.getFetchDirection() );
        assertEquals( expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize() );
        assertEquals( expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows() );
        assertEquals( expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing() );
        assertEquals( expectedProperties.isPoolable(), actualProperties.isPoolable() );
    }


    @Test
    public void toStatementProperties_Defaults_Conversion() throws SQLException {
        when( polyphenyStatement.hasStatementId() ).thenReturn( false );
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement( polyphenyStatement );
        expectedProperties.setPrismInterfaceClient( PRISM_INTERFACE_CLIENT );
        expectedProperties.setQueryTimeoutSeconds( PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS() );
        expectedProperties.setResultSetType( ResultSet.TYPE_FORWARD_ONLY );
        expectedProperties.setResultSetConcurrency( ResultSet.CONCUR_READ_ONLY );
        expectedProperties.setResultSetHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        expectedProperties.setFetchSize( PropertyUtils.getDEFAULT_FETCH_SIZE() );
        expectedProperties.setFetchDirection( PropertyUtils.getDEFAULT_FETCH_DIRECTION() );
        expectedProperties.setMaxFieldSize( PropertyUtils.getDEFAULT_MAX_FIELD_SIZE() );
        expectedProperties.setLargeMaxRows( PropertyUtils.getDEFAULT_LARGE_MAX_ROWS() );
        expectedProperties.setDoesEscapeProcessing( PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING() );
        expectedProperties.setIsPoolable( PropertyUtils.isDEFAULT_STATEMENT_POOLABLE() );

        try {
            PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties();
            actualProperties.setPolyphenyStatement( polyphenyStatement );
            assertEquals( expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds() );
            assertEquals( expectedProperties.getResultSetType(), actualProperties.getResultSetType() );
            assertEquals( expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency() );
            assertEquals( expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability() );
            assertEquals( expectedProperties.getFetchSize(), actualProperties.getFetchSize() );
            assertEquals( expectedProperties.getFetchDirection(), actualProperties.getFetchDirection() );
            assertEquals( expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize() );
            assertEquals( expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows() );
            assertEquals( expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing() );
            assertEquals( expectedProperties.isPoolable(), actualProperties.isPoolable() );
        } catch ( SQLException e ) {
            fail( "SQLException thrown: " + e.getMessage() );
        }
    }


    @Test
    public void setNamespaceName_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        String namespaceName = "testNamespace";

        connectionProperties.setNamespaceName( namespaceName );

        assertEquals( namespaceName, connectionProperties.getNamespaceName() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setCatalogName_Valid_NoSync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        String catalogName = "test_catalog";

        connectionProperties.setCatalogName( catalogName );

        assertEquals( catalogName, connectionProperties.getCatalogName() );
        verify( PRISM_INTERFACE_CLIENT, times( 0 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setTransactionIsolation_Invalid_Error() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );

        try {
            connectionProperties.setTransactionIsolation( 999 );
            fail( "Expected SQLException to be thrown" );
        } catch ( SQLException e ) {
            assertEquals( "Invalid value for transaction isolation", e.getMessage() );
        }
    }


    @Test
    public void setTransactionIsolation_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;

        try {
            connectionProperties.setTransactionIsolation( transactionIsolation );
        } catch ( SQLException e ) {
            fail( "Should not throw an exception" );
        }

        assertEquals( transactionIsolation, connectionProperties.getTransactionIsolation() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setNetworkTimeout_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        int networkTimeout = 5000;

        connectionProperties.setNetworkTimeout( networkTimeout );

        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
        assertEquals( connectionProperties.getNetworkTimeout(), networkTimeout );
    }


    @Test
    public void setResultSetHoldability_Invalid_Error() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );

        try {
            connectionProperties.setResultSetHoldability( 100 );
        } catch ( SQLException e ) {
            assertTrue( e.getMessage().contains( "Invalid value for result set holdability" ) );
        }
    }


    @Test
    public void setResultSetHoldability_Valid_NoSync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );

        connectionProperties.setResultSetHoldability( ResultSet.CLOSE_CURSORS_AT_COMMIT );
        assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, connectionProperties.getResultSetHoldability() );

        verify( PRISM_INTERFACE_CLIENT, times( 0 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setReadOnly_False_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        connectionProperties.setReadOnly( false );

        assertFalse( connectionProperties.isReadOnly() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setReadOnly_True_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        connectionProperties.setReadOnly( true );

        assertTrue( connectionProperties.isReadOnly() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setAutoCommit_False_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        connectionProperties.setAutoCommit( false );

        assertFalse( connectionProperties.isAutoCommit() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }


    @Test
    public void setAutoCommit_True_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, PRISM_INTERFACE_CLIENT );
        connectionProperties.setAutoCommit( true );

        assertTrue( connectionProperties.isAutoCommit() );
        verify( PRISM_INTERFACE_CLIENT, times( 1 ) ).setConnectionProperties( connectionProperties, connectionProperties.getNetworkTimeout() );
    }

}

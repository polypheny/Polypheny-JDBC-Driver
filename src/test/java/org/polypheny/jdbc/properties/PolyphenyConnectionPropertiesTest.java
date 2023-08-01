package org.polypheny.jdbc.properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.jdbc.ConnectionString;
import org.polypheny.jdbc.PolyphenyStatement;
import org.polypheny.jdbc.ProtoInterfaceClient;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class PolyphenyConnectionPropertiesTest {
    private static ConnectionString connectionString;

    @BeforeClass
    public static void setUpClass() throws SQLException {
        connectionString = new ConnectionString("jdbc:polypheny://localhost:20590");
    }


    private static final ProtoInterfaceClient protoInterfaceClient = mock(ProtoInterfaceClient.class);
    private static final PolyphenyStatement polyphenyStatement = mock(PolyphenyStatement.class);


    @Test
    public void toStatementProperties_Type_Concurrency_Holdability_Conversion() throws SQLException {
        when(polyphenyStatement.hasStatementId()).thenReturn(false);
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement(polyphenyStatement);
        expectedProperties.setProtoInterfaceClient(protoInterfaceClient);
        expectedProperties.setQueryTimeoutSeconds(PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS());
        expectedProperties.setResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE);
        expectedProperties.setResultSetConcurrency(ResultSet.CONCUR_READ_ONLY);
        expectedProperties.setResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        expectedProperties.setFetchSize(PropertyUtils.getDEFAULT_FETCH_SIZE());
        expectedProperties.setFetchDirection(PropertyUtils.getDEFAULT_FETCH_DIRECTION());
        expectedProperties.setMaxFieldSize(PropertyUtils.getDEFAULT_MAX_FIELD_SIZE());
        expectedProperties.setLargeMaxRows(PropertyUtils.getDEFAULT_LARGE_MAX_ROWS());
        expectedProperties.setDoesEscapeProcessing(PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING());
        expectedProperties.setIsPoolable(PropertyUtils.isDEFAULT_STATEMENT_POOLABLE());

        PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties(
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY,
                ResultSet.CLOSE_CURSORS_AT_COMMIT
        );
        actualProperties.setPolyphenyStatement(polyphenyStatement);
        assertEquals(expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds());
        assertEquals(expectedProperties.getResultSetType(), actualProperties.getResultSetType());
        assertEquals(expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency());
        assertEquals(expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability());
        assertEquals(expectedProperties.getFetchSize(), actualProperties.getFetchSize());
        assertEquals(expectedProperties.getFetchDirection(), actualProperties.getFetchDirection());
        assertEquals(expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize());
        assertEquals(expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows());
        assertEquals(expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing());
        assertEquals(expectedProperties.isPoolable(), actualProperties.isPoolable());
    }

    @Test
    public void toStatementProperties_Type_Concurrency_Conversion() throws SQLException {
        when(polyphenyStatement.hasStatementId()).thenReturn(false);
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement(polyphenyStatement);
        expectedProperties.setProtoInterfaceClient(protoInterfaceClient);
        expectedProperties.setQueryTimeoutSeconds(PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS());
        expectedProperties.setResultSetType(ResultSet.TYPE_FORWARD_ONLY);
        expectedProperties.setResultSetConcurrency(ResultSet.CONCUR_READ_ONLY);
        expectedProperties.setResultSetHoldability(PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY());
        expectedProperties.setFetchSize(PropertyUtils.getDEFAULT_FETCH_SIZE());
        expectedProperties.setFetchDirection(PropertyUtils.getDEFAULT_FETCH_DIRECTION());
        expectedProperties.setMaxFieldSize(PropertyUtils.getDEFAULT_MAX_FIELD_SIZE());
        expectedProperties.setLargeMaxRows(PropertyUtils.getDEFAULT_LARGE_MAX_ROWS());
        expectedProperties.setDoesEscapeProcessing(PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING());
        expectedProperties.setIsPoolable(PropertyUtils.isDEFAULT_STATEMENT_POOLABLE());

        PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        actualProperties.setPolyphenyStatement(polyphenyStatement);
        assertEquals(expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds());
        assertEquals(expectedProperties.getResultSetType(), actualProperties.getResultSetType());
        assertEquals(expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency());
        assertEquals(expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability());
        assertEquals(expectedProperties.getFetchSize(), actualProperties.getFetchSize());
        assertEquals(expectedProperties.getFetchDirection(), actualProperties.getFetchDirection());
        assertEquals(expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize());
        assertEquals(expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows());
        assertEquals(expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing());
        assertEquals(expectedProperties.isPoolable(), actualProperties.isPoolable());
    }


    @Test
    public void toStatementProperties_Defaults_Conversion() throws SQLException {
        when(polyphenyStatement.hasStatementId()).thenReturn(false);
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        PolyphenyStatementProperties expectedProperties = new PolyphenyStatementProperties();
        expectedProperties.setPolyphenyStatement(polyphenyStatement);
        expectedProperties.setProtoInterfaceClient(protoInterfaceClient);
        expectedProperties.setQueryTimeoutSeconds(PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS());
        expectedProperties.setResultSetType(ResultSet.TYPE_FORWARD_ONLY);
        expectedProperties.setResultSetConcurrency(ResultSet.CONCUR_READ_ONLY);
        expectedProperties.setResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        expectedProperties.setFetchSize(PropertyUtils.getDEFAULT_FETCH_SIZE());
        expectedProperties.setFetchDirection(PropertyUtils.getDEFAULT_FETCH_DIRECTION());
        expectedProperties.setMaxFieldSize(PropertyUtils.getDEFAULT_MAX_FIELD_SIZE());
        expectedProperties.setLargeMaxRows(PropertyUtils.getDEFAULT_LARGE_MAX_ROWS());
        expectedProperties.setDoesEscapeProcessing(PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING());
        expectedProperties.setIsPoolable(PropertyUtils.isDEFAULT_STATEMENT_POOLABLE());

        try {
            PolyphenyStatementProperties actualProperties = connectionProperties.toStatementProperties();
            actualProperties.setPolyphenyStatement(polyphenyStatement);
            assertEquals(expectedProperties.getQueryTimeoutSeconds(), actualProperties.getQueryTimeoutSeconds());
            assertEquals(expectedProperties.getResultSetType(), actualProperties.getResultSetType());
            assertEquals(expectedProperties.getResultSetConcurrency(), actualProperties.getResultSetConcurrency());
            assertEquals(expectedProperties.getResultSetHoldability(), actualProperties.getResultSetHoldability());
            assertEquals(expectedProperties.getFetchSize(), actualProperties.getFetchSize());
            assertEquals(expectedProperties.getFetchDirection(), actualProperties.getFetchDirection());
            assertEquals(expectedProperties.getMaxFieldSize(), actualProperties.getMaxFieldSize());
            assertEquals(expectedProperties.getLargeMaxRows(), actualProperties.getLargeMaxRows());
            assertEquals(expectedProperties.isDoesEscapeProcessing(), actualProperties.isDoesEscapeProcessing());
            assertEquals(expectedProperties.isPoolable(), actualProperties.isPoolable());
        } catch (SQLException e) {
            fail("SQLException thrown: " + e.getMessage());
        }
    }

    @Test
    public void setNamespaceName_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        String namespaceName = "testNamespace";

        connectionProperties.setNamespaceName(namespaceName);

        assertEquals(namespaceName, connectionProperties.getNamespaceName());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }

    @Test
    public void setCatalogName_Valid_NoSync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        String catalogName = "test_catalog";

        connectionProperties.setCatalogName(catalogName);

        assertEquals(catalogName, connectionProperties.getCatalogName());
        verify(protoInterfaceClient, times(0)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }

    @Test
    public void setTransactionIsolation_Invalid_Error() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);

        try {
            connectionProperties.setTransactionIsolation(999);
            fail("Expected SQLException to be thrown");
        } catch (SQLException e) {
            assertEquals("Invalid value for transaction isolation", e.getMessage());
        }
    }

    @Test
    public void setTransactionIsolation_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        int transactionIsolation = Connection.TRANSACTION_READ_COMMITTED;

        try {
            connectionProperties.setTransactionIsolation(transactionIsolation);
        } catch (SQLException e) {
            fail("Should not throw an exception");
        }

        assertEquals(transactionIsolation, connectionProperties.getTransactionIsolation());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }

    @Test
    public void setNetworkTimeout_Valid_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        int networkTimeout = 5000;

        connectionProperties.setNetworkTimeout(networkTimeout);

        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
        assertEquals(connectionProperties.getNetworkTimeout(), networkTimeout);
    }

    @Test
    public void setResultSetHoldability_Invalid_Error() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);

        try {
            connectionProperties.setResultSetHoldability(100);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("Invalid value for result set holdability"));
        }
    }

    @Test
    public void setResultSetHoldability_Valid_NoSync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);

        try {
            connectionProperties.setResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, connectionProperties.getResultSetHoldability());

        verify(protoInterfaceClient, times(0)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }


    @Test
    public void setReadOnly_False_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        connectionProperties.setReadOnly(false);

        assertFalse(connectionProperties.isReadOnly());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }

    @Test
    public void setReadOnly_True_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        connectionProperties.setReadOnly(true);

        assertTrue(connectionProperties.isReadOnly());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }


    @Test
    public void setAutoCommit_False_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        connectionProperties.setAutoCommit(false);

        assertFalse(connectionProperties.isAutoCommit());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }

    @Test
    public void setAutoCommit_True_Sync() throws SQLException {
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        connectionProperties.setAutoCommit(true);

        assertTrue(connectionProperties.isAutoCommit());
        verify(protoInterfaceClient, times(1)).setConnectionProperties(connectionProperties, connectionProperties.getNetworkTimeout());
    }
}
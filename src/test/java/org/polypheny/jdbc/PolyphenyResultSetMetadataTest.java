package org.polypheny.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PolyphenyResultSetMetadataTest {

    private static ArrayList<PolyphenyColumnMeta> columnMetas;
    private static PolyphenyResultSetMetadata resultSetMetadata;


    @BeforeClass
    public static void setUpClass() {
        PolyphenyColumnMeta firstMeta = PolyphenyColumnMeta.fromSpecification( 0, "first", "unittest", Types.BIGINT );
        PolyphenyColumnMeta secondMeta = PolyphenyColumnMeta.fromSpecification( 1, "second", "unittest", Types.VARCHAR );
        columnMetas = new ArrayList<>( Arrays.asList( firstMeta, secondMeta ) );
        resultSetMetadata = new PolyphenyResultSetMetadata( columnMetas );
    }


    @AfterClass
    public static void tearDownClass() {
    }


    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {

    }


    @Test
    public void getColumnCount_2() throws SQLException {
        assertEquals( 2, resultSetMetadata.getColumnCount() );
    }


    @Test(expected = SQLException.class)
    public void accessOutOfBounds__ColumnIndex_Exception() throws SQLException {
        resultSetMetadata.isAutoIncrement( 100 );
        fail( "No sql exception thrown" );
    }


    @Test
    public void isAutoIncrement__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isAutoIncrement( 1 ) );
    }


    @Test
    public void isCaseSensitive__ColumnIndex_true() throws SQLException {
        assertTrue( resultSetMetadata.isCaseSensitive( 1 ) );
    }

    @Test
    public void isSearchable__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isSearchable( 1 ) );
    }

    @Test
    public void isCurrency__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isCurrency( 1 ) );
    }

    @Test
    public void isNullable__ColumnIndex_columnNullable() throws SQLException {
        assertEquals( ResultSetMetaData.columnNullable, resultSetMetadata.isNullable( 1 ) );
    }

    @Test
    public void isSigned__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isSigned( 1 ) );
    }

    @Test
    public void getColumnDisplaySize__ColumnIndex_false() throws SQLException {
        assertEquals( -1, resultSetMetadata.getColumnDisplaySize( 1 ) );
    }

    @Test
    public void getColumnLabel__ColumnIndex_false() throws SQLException {
        assertEquals( "first", resultSetMetadata.getColumnLabel( 1 ) );
    }

    @Test
    public void getColumnName__ColumnIndex_false() throws SQLException {
        assertNull( resultSetMetadata.getColumnName( 1 ) );
    }

    @Test
    public void getSchemaName__ColumnIndex_false() throws SQLException {
        assertNull( resultSetMetadata.getSchemaName( 1 ) );
    }

    @Test
    public void getPrecision__ColumnIndex_false() throws SQLException {
        assertEquals( -1, resultSetMetadata.getPrecision( 1 ) );
    }

    @Test
    public void getScale__ColumnIndex_false() throws SQLException {
        assertEquals( 1, resultSetMetadata.getScale( 1 ) );
    }

    @Test
    public void getTableName__ColumnIndex_false() throws SQLException {
        assertEquals( "unittest", resultSetMetadata.getTableName( 1 ) );
    }

    @Test
    public void getCatalogName__ColumnIndex_false() throws SQLException {
        assertEquals( "", resultSetMetadata.getCatalogName( 1 ) );
    }

    @Test
    public void getColumnType__ColumnIndex_false() throws SQLException {
        assertEquals( Types.VARCHAR, resultSetMetadata.getColumnType( 2 ) );
    }

    @Test
    public void isReadOnly__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isReadOnly( 1 ) );
    }

    @Test
    public void isWritable__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isWritable( 1 ) );
    }

    @Test
    public void isDefinitelyWritable__ColumnIndex_false() throws SQLException {
        assertFalse( resultSetMetadata.isDefinitelyWritable( 1 ) );
    }

    @Test
    public void getColumnClassName__ColumnIndex_false() throws SQLException {
        assertEquals( "", resultSetMetadata.getColumnClassName( 1 ) );
    }

    @Test
    public void getIndexFromLabel__ColumnIndex_false() throws SQLException {
        assertEquals( 1, resultSetMetadata.getColumnIndexFromLabel( "first" ) );
    }


}

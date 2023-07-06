package org.polypheny.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetaScrollerTest {

    private static final ArrayList<Integer> TEST_DATA_EMPTY = new ArrayList<>();
    private static final ArrayList<Integer> TEST_DATA_FOUR = new ArrayList<>( Arrays.asList( 1, 2, 3, 4 ) );


    @BeforeClass
    public static void setUpClass() {
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
    public void indexInitPosition__empty_beforeFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertTrue( scroller.isBeforeFirst() );
    }


    @Test
    public void indexInitPosition__empty_notFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.isFirst() );
    }


    @Test
    public void indexInitPosition__empty_notAfterLastFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.isAfterLast() );
    }


    @Test
    public void indexInitPosition__empty_notLast() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertTrue( scroller.isLast() );
    }


    @Test
    public void getRowInit__empty_0() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void currentInit__empty_null() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertNull( scroller.current() );
    }


    @Test
    public void next__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.next() );
    }


    @Test
    public void nextGetRow__empty_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void nextCurrent__empty_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertNull( scroller.current() );
    }


    @Test
    public void previous__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.previous() );
    }


    @Test
    public void previousGetRow__empty_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void previousCurrent__empty_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertNull( scroller.current() );
    }


    @Test
    public void absolute0__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.absolute( 0 ) );
    }


    @Test
    public void absolute0Current__empty_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertNull( scroller.current() );
    }


    @Test
    public void absolute0IsBefore__empty_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.absolute( 0 );
        assertTrue( scroller.isBeforeFirst() );
    }


    @Test
    public void absolute5IsAfter__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.absolute( 5 ) );
    }


    @Test
    public void absolute5IsAfter__empty_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.absolute( 5 );
        assertTrue( scroller.isAfterLast() );
    }


    @Test
    public void absoluteMinus5IsBefore__empty_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.absolute( -5 );
        assertTrue( scroller.isBeforeFirst() );
    }


    @Test
    public void absoluteMinus5IsBefore__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.absolute( -5 ) );
    }


    @Test
    public void relative1IsAfter__empty_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.relative( 1 );
        assertTrue( scroller.isAfterLast() );
    }


    @Test
    public void relative1__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.relative( 1 ) );
    }


    @Test
    public void relativeMinus1__empty_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.relative( -1 ) );
    }


    @Test
    public void relative1Current__empty_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.relative( 1 );
        assertNull( scroller.current() );
    }


    @Test
    public void relativeMinus1Current__empty_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.relative( -1 );
        assertNull( scroller.current() );
    }


    @Test
    public void relative1GetRow__empty_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.relative( 1 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void relativeMinus1GetRow__empty_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        scroller.relative( -1 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void next__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        assertTrue( scroller.next() );
    }


    @Test
    public void nextGetRow__data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        assertEquals( 1, scroller.getRow() );
    }


    @Test
    public void nextCurrent__data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        assertEquals( Integer.valueOf( 1 ), scroller.current() );
    }


    @Test
    public void atFirst_data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        assertTrue( scroller.isFirst() );
    }


    @Test
    public void atLast_data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 4 );
        assertTrue( scroller.isLast() );
    }


    @Test
    public void previous__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        assertFalse( scroller.previous() );
    }


    @Test
    public void previousGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.previous();
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void previousCurrent__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.previous();
        assertNull( scroller.current() );
    }


    @Test
    public void nextOverflow__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.next();
        scroller.next();
        assertFalse( scroller.next() );
    }


    @Test
    public void nextOverflowCurrent__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.next();
        scroller.next();
        scroller.next();
        assertNull( scroller.current() );
    }


    @Test
    public void nextOverflowGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.next();
        scroller.next();
        scroller.next();
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void nextCurrent__data_value() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        for ( int i = 0; i < 4; i++ ) {
            scroller.next();
            assertEquals( Integer.valueOf( i + 1 ), scroller.current() );
        }
    }


    @Test
    public void rel0Invalid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        assertFalse( scroller.relative( 0 ) );
    }


    @Test
    public void rel0Valid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        assertTrue( scroller.relative( 0 ) );
    }


    @Test
    public void rel0InvalidCurrent__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.relative( 0 );
        assertNull( scroller.current() );
    }


    @Test
    public void rel0ValidCurrent__data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.relative( 0 );
        assertEquals( Integer.valueOf( 1 ), scroller.current() );
    }


    @Test
    public void relIncOverflowValid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        assertFalse( scroller.relative( 3 ) );
    }


    @Test
    public void relDecUnderflowValid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        assertFalse( scroller.relative( -3 ) );
    }


    @Test
    public void relIncOverflowValue__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( 3 );
        assertNull( scroller.current() );
    }


    @Test
    public void relDecUnderflowValue__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( -3 );
        assertNull( scroller.current() );
    }


    @Test
    public void relIncOverflowAfterLast__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( 3 );
        assertTrue( scroller.isAfterLast() );
    }


    @Test
    public void relDecUnderflowBeforeFirst__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( -3 );
        assertTrue( scroller.isBeforeFirst() );
    }


    @Test
    public void relIncOverflowGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( 3 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void relDecUnderflowGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.next();
        scroller.next();
        scroller.relative( -3 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void relIncValid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        assertTrue( scroller.relative( 3 ) );
    }


    @Test
    public void relDecwValid__data_false() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 4 );
        assertTrue( scroller.relative( -3 ) );
    }


    @Test
    public void relIncValue__data_4() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.relative( 4 );
        assertEquals( Integer.valueOf( 4 ), scroller.current() );
    }


    @Test
    public void relDecValue__data_1() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 4 );
        scroller.relative( -3 );
        assertEquals( Integer.valueOf( 1 ), scroller.current() );
    }


    @Test
    public void absOverflowValue__data_null() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 5 );
        assertNull( scroller.current() );
    }


    @Test
    public void absUnderflowValue__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.relative( -5 );
        assertNull( scroller.current() );
    }


    @Test
    public void absUnderflowBeforeFirst__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.relative( -5 );
        assertTrue( scroller.isBeforeFirst() );
    }


    @Test
    public void absOverflowAfterLast__data_true() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 5 );
        assertTrue( scroller.isAfterLast() );
    }


    @Test
    public void absOverflowGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( 5 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void absUnderflowGetRow__data_0() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( -5 );
        assertEquals( 0, scroller.getRow() );
    }


    @Test
    public void absReverseAccessValue__data_3() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( -2 );
        assertEquals( Integer.valueOf( 3 ), scroller.current() );
    }


    @Test
    public void absReverseAccessGetRow__data_3() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        scroller.absolute( -2 );
        assertEquals( 3, scroller.getRow() );
    }


    @Test
    public void absReverseAccessValid__data_3() throws SQLException, InterruptedException {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_FOUR );
        assertTrue( scroller.absolute( -2 ) );
    }


}
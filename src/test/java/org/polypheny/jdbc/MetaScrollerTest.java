/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.jdbc;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.meta.MetaScroller;

public class MetaScrollerTest {

    private static final List<Integer> TEST_DATA_EMPTY = Collections.emptyList();
    private static final List<Integer> TEST_DATA_FOUR = new ArrayList<>();


    static {
        TEST_DATA_FOUR.add( 1 );
        TEST_DATA_FOUR.add( 2 );
        TEST_DATA_FOUR.add( 3 );
        TEST_DATA_FOUR.add( 4 );
    }


    @Test
    public void indexInitPosition__empty_beforeFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertTrue( scroller.isBeforeFirst() );
        assertFalse( scroller.hasCurrent() );
    }


    @Test
    public void indexInitPosition__empty_notFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertFalse( scroller.isFirst() );
    }


    @Test
    public void indexInitPosition__empty_notAfterLastFirst() {
        MetaScroller<Integer> scroller = new MetaScroller<>( TEST_DATA_EMPTY );
        assertTrue( scroller.isAfterLast() );
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

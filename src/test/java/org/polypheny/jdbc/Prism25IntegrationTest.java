/*
 * Copyright 2019-2026 The Polypheny Project
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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.types.PolyBlob;

@Tag("prism25")
class Prism25IntegrationTest {

    private static final String DB_URL = "jdbc:polypheny://127.0.0.1:" + System.getenv().getOrDefault( "POLYPHENY_TEST_PORT", "20590" );


    @Test
    void preSendQueryTimeoutPreservesEarlierWrites() throws SQLException {
        String table = "jdbc_deadline_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection observer = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement ddl = observer.createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, payload VARBINARY(10))" );
            try {
                try ( Connection writer = DriverManager.getConnection( DB_URL, "pa", "" );
                        Statement earlier = writer.createStatement();
                        PreparedStatement later = writer.prepareStatement( "INSERT INTO " + table + " (id, payload) VALUES (2, ?)" ) ) {
                    writer.setAutoCommit( false );
                    assertEquals( 1, earlier.executeUpdate( "INSERT INTO " + table + " (id) VALUES (1)" ) );
                    later.setBlob( 1, new PolyBlob( new byte[]{ 1 } ) {
                        @Override
                        public InputStream getBinaryStream() throws SQLException {
                            try {
                                TimeUnit.MILLISECONDS.sleep( 1200 );
                            } catch ( InterruptedException e ) {
                                Thread.currentThread().interrupt();
                                throw new SQLException( e );
                            }
                            return super.getBinaryStream();
                        }
                    } );
                    later.setQueryTimeout( 1 );
                    assertThrows( SQLTimeoutException.class, later::executeUpdate );
                    assertFalse( writer.isClosed() );
                    writer.commit();
                }
                try ( ResultSet rows = ddl.executeQuery( "SELECT id FROM " + table ) ) {
                    assertTrue( rows.next() );
                    assertEquals( 1, rows.getInt( 1 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void nationalCharacterStreamHonorsItsDeclaredLength() throws SQLException {
        String table = "jdbc_nchars_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement ddl = connection.createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id INTEGER PRIMARY KEY, label VARCHAR(30))" );
            try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO " + table + " VALUES (1, ?)" ) ) {
                assertThrows( SQLException.class, () -> insert.setNCharacterStream( 1, new StringReader( "short" ), 6 ) );
                assertThrows( SQLException.class, () -> insert.setNCharacterStream( 1, new StringReader( "short" ), -1 ) );
                insert.setNCharacterStream( 1, new StringReader( "préfixe-tail" ), 6 );
                assertEquals( 1, insert.executeUpdate() );

                try ( ResultSet rows = ddl.executeQuery( "SELECT label FROM " + table ) ) {
                    assertTrue( rows.next() );
                    assertEquals( "préfix", rows.getString( 1 ) );
                    assertFalse( rows.next() );
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void generatedKeysRespectCursorContractAndBatchOrder() throws SQLException {
        String table = "jdbc_keys_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement ddl = connection.createStatement();
                Statement insert = connection.createStatement() ) {
            assertTrue( connection.getMetaData().supportsGetGeneratedKeys() );
            ddl.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, label TEXT NOT NULL)" );
            try {
                insert.setMaxRows( 1 );
                assertEquals( 2, insert.executeUpdate(
                        "INSERT INTO " + table + " (label) VALUES ('first'), ('second')", Statement.RETURN_GENERATED_KEYS ) );
                try ( ResultSet keys = insert.getGeneratedKeys() ) {
                    assertTrue( keys.getMetaData().isAutoIncrement( 1 ) );
                    assertTrue( keys.next() );
                    assertEquals( 1, keys.getLong( 1 ) );
                    assertFalse( keys.next() );
                }

                assertEquals( 1, insert.executeUpdate(
                        "INSERT INTO " + table + " (id, label) VALUES (99, 'explicit')", Statement.RETURN_GENERATED_KEYS ) );
                try ( ResultSet keys = insert.getGeneratedKeys() ) {
                    assertNoCursorPosition( keys );
                    assertFalse( keys.next() );
                    assertNoCursorPosition( keys );
                }

                try ( PreparedStatement batch = connection.prepareStatement(
                        "INSERT INTO " + table + " (label) VALUES (?)", new String[]{ "label", "id" } ) ) {
                    batch.setString( 1, "third" );
                    batch.addBatch();
                    batch.setString( 1, "fourth" );
                    batch.addBatch();
                    assertArrayEquals( new int[]{ 1, 1 }, batch.executeBatch() );
                    try ( ResultSet keys = batch.getGeneratedKeys() ) {
                        assertTrue( keys.next() );
                        assertEquals( "third", keys.getString( 1 ) );
                        assertEquals( 3, keys.getLong( 2 ) );
                        assertTrue( keys.next() );
                        assertEquals( "fourth", keys.getString( 1 ) );
                        assertEquals( 4, keys.getLong( 2 ) );
                        assertFalse( keys.next() );
                    }
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void commitClosesGeneratedKeyCursors( boolean enableAutoCommit ) throws SQLException {
        String table = "jdbc_keys_commit_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection observer = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement ddl = observer.createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY)" );
            String sql = "INSERT INTO " + table + " DEFAULT VALUES";
            try {
                try ( Connection writer = DriverManager.getConnection( DB_URL, "pa", "" );
                        Statement raw = writer.createStatement();
                        PreparedStatement prepared = writer.prepareStatement( sql, Statement.RETURN_GENERATED_KEYS ) ) {
                    writer.setAutoCommit( false );
                    prepared.closeOnCompletion();
                    assertEquals( 1, raw.executeUpdate( sql, Statement.RETURN_GENERATED_KEYS ) );
                    assertEquals( 1, prepared.executeUpdate() );
                    try ( ResultSet rawKeys = raw.getGeneratedKeys(); ResultSet preparedKeys = prepared.getGeneratedKeys() ) {
                        assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, rawKeys.getHoldability() );
                        assertTrue( preparedKeys.next() );
                        if ( enableAutoCommit ) {
                            writer.setAutoCommit( true );
                        } else {
                            writer.commit();
                        }
                        assertEquals( enableAutoCommit, writer.getAutoCommit() );
                        assertTrue( rawKeys.isClosed() );
                        assertTrue( preparedKeys.isClosed() );
                        assertThrows( SQLException.class, rawKeys::next );
                        assertTrue( prepared.isClosed() );
                        assertFalse( raw.isClosed() );
                    }
                    try ( ResultSet cleared = raw.getGeneratedKeys() ) {
                        assertFalse( cleared.next() );
                    }
                    try ( Connection reader = DriverManager.getConnection( DB_URL, "pa", "" );
                            Statement query = reader.createStatement() ) {
                        query.setQueryTimeout( 2 );
                        try ( ResultSet count = query.executeQuery( "SELECT COUNT(*) FROM " + table ) ) {
                            assertTrue( count.next() );
                            assertEquals( 2, count.getInt( 1 ) );
                        }
                    }
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void commitClosesPagedQueryCursors( boolean enableAutoCommit ) throws SQLException {
        String sql = "SELECT n FROM (VALUES (1), (2), (3)) AS t(n) ORDER BY n";
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" ) ) {
            connection.setAutoCommit( false );
            try ( PreparedStatement reusable = connection.prepareStatement( sql );
                    PreparedStatement completed = connection.prepareStatement( sql ) ) {
                reusable.setFetchSize( 1 );
                completed.setFetchSize( 1 );
                completed.closeOnCompletion();
                try ( ResultSet rows = reusable.executeQuery(); ResultSet completedRows = completed.executeQuery() ) {
                    assertEquals( ResultSet.CLOSE_CURSORS_AT_COMMIT, rows.getHoldability() );
                    assertFalse( rows.isClosed() );
                    if ( enableAutoCommit ) {
                        connection.setAutoCommit( true );
                    } else {
                        connection.commit();
                    }
                    assertTrue( rows.isClosed() );
                    assertTrue( completedRows.isClosed() );
                    assertThrows( SQLException.class, rows::next );
                    assertFalse( reusable.isClosed() );
                    assertNull( reusable.getResultSet() );
                    assertTrue( completed.isClosed() );
                }
                try ( ResultSet next = reusable.executeQuery() ) {
                    assertTrue( next.next() );
                    assertEquals( 1, next.getInt( 1 ) );
                }
            }
        }
    }


    @Test
    void rawGeneratedKeySelectorsRespectNumericTypes() throws SQLException {
        String table = "jdbc_raw_numeric_keys_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, small_num SMALLINT, qty INTEGER, "
                    + "wide_num BIGINT, ratio REAL, approximation DOUBLE, amount DECIMAL(12,4), optional_num INTEGER)" );
            try {
                String insert = "INSERT INTO " + table + " (small_num, qty, wide_num, ratio, approximation, amount) "
                        + "VALUES (128, 42.6, 9007199254740993, 1.5, 2.5, 1.2)";
                assertEquals( 1, sql.executeUpdate( insert, new int[]{ 2, 3, 4, 5, 6, 7, 8 } ) );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertNumericProjection( keys );
                }
                assertEquals( 1, sql.executeUpdate( insert,
                        new String[]{ "small_num", "qty", "wide_num", "ratio", "approximation", "amount", "optional_num" } ) );
                try ( ResultSet keys = sql.getGeneratedKeys() ) {
                    assertNumericProjection( keys );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    private static void assertNumericProjection( ResultSet keys ) throws SQLException {
        long exactLong = 9_007_199_254_740_993L;
        BigDecimal amount = new BigDecimal( "1.2000" );
        assertTrue( keys.next() );
        assertEquals( (short) 128, keys.getShort( "small_num" ) );
        assertEquals( Integer.valueOf( 128 ), keys.getObject( "small_num", Integer.class ) );
        assertEquals( "22003", assertThrows( SQLException.class, () -> keys.getByte( "small_num" ) ).getSQLState() );
        assertEquals( 43, keys.getInt( "qty" ) );
        assertEquals( Integer.valueOf( 43 ), keys.getObject( "qty" ) );
        assertEquals( BigDecimal.valueOf( 43 ), keys.getBigDecimal( "qty" ) );
        assertEquals( exactLong, keys.getLong( "wide_num" ) );
        assertEquals( Long.valueOf( exactLong ), keys.getObject( "wide_num", Long.class ) );
        assertEquals( "22003", assertThrows( SQLException.class, () -> keys.getInt( "wide_num" ) ).getSQLState() );
        assertEquals( 1.5f, keys.getFloat( "ratio" ) );
        assertEquals( Float.valueOf( 1.5f ), keys.getObject( "ratio", Float.class ) );
        assertEquals( 2.5d, keys.getDouble( "approximation" ) );
        assertEquals( Double.valueOf( 2.5d ), keys.getObject( "approximation", Double.class ) );
        assertEquals( amount, keys.getBigDecimal( "amount" ) );
        assertEquals( amount, keys.getObject( "amount", BigDecimal.class ) );
        assertNull( keys.getObject( "optional_num", Integer.class ) );
        assertTrue( keys.wasNull() );
        assertFalse( keys.next() );
    }


    @Test
    void preparedGeneratedKeysConvertIntegralWireValuesToFloatingTypes() throws SQLException {
        String table = "jdbc_prepared_numeric_keys_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement ddl = connection.createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, ratio REAL, approximation DOUBLE)" );
            try ( PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + table + " (ratio, approximation) VALUES (?, ?)", new String[]{ "ratio", "approximation" } ) ) {
                int realInput = 16_777_217;
                long doubleInput = 9_007_199_254_740_993L;
                insert.setInt( 1, realInput );
                insert.setLong( 2, doubleInput );
                assertEquals( 1, insert.executeUpdate() );
                try ( ResultSet keys = insert.getGeneratedKeys() ) {
                    assertTrue( keys.next() );
                    assertEquals( (float) realInput, keys.getFloat( "ratio" ) );
                    assertEquals( (double) (float) realInput, keys.getDouble( "ratio" ) );
                    assertEquals( Float.valueOf( realInput ), keys.getObject( "ratio", Float.class ) );
                    assertEquals( (double) doubleInput, keys.getDouble( "approximation" ) );
                    assertEquals( Double.valueOf( doubleInput ), keys.getObject( "approximation", Double.class ) );
                    assertFalse( keys.next() );
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void readsPreparedFloatingKeysLikeThePersistedRow() throws SQLException {
        String table = "jdbc_float_key_readback_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, ratio REAL, approximation DOUBLE)" );
            try ( PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + table + " (ratio, approximation) VALUES (?, ?)", new String[]{ "ratio", "approximation" } ) ) {
                insert.setDouble( 1, 16_777_217.25d );
                insert.setLong( 2, 9_007_199_254_740_993L );
                assertEquals( 1, insert.executeUpdate() );
                try ( ResultSet keys = insert.getGeneratedKeys();
                        ResultSet stored = sql.executeQuery( "SELECT ratio, approximation FROM " + table ) ) {
                    assertTrue( keys.next() );
                    assertTrue( stored.next() );
                    assertEquals( 16_777_218d, stored.getDouble( "ratio" ) );
                    assertEquals( stored.getDouble( "ratio" ), keys.getDouble( "ratio" ) );
                    assertEquals( stored.getString( "ratio" ), keys.getString( "ratio" ) );
                    assertEquals( stored.getString( "approximation" ), keys.getString( "approximation" ) );
                    assertEquals( stored.getObject( "ratio", Float.class ), keys.getObject( "ratio", Float.class ) );
                    assertEquals( 9_007_199_254_740_992L, keys.getLong( "approximation" ) );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void preparedDecimalKeyMatchesThePersistedScale() throws SQLException {
        String table = "jdbc_decimal_key_readback_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, amount DECIMAL(12,4))" );
            try ( PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + table + " (amount) VALUES (?)", new String[]{ "amount" } ) ) {
                insert.setBigDecimal( 1, new BigDecimal( "1.234567" ) );
                assertEquals( 1, insert.executeUpdate() );
                try ( ResultSet keys = insert.getGeneratedKeys(); ResultSet stored = sql.executeQuery( "SELECT amount FROM " + table ) ) {
                    assertTrue( keys.next() );
                    assertTrue( stored.next() );
                    assertEquals( 4, keys.getMetaData().getScale( 1 ) );
                    assertEquals( new BigDecimal( "1.2346" ), stored.getBigDecimal( 1 ) );
                    assertEquals( stored.getBigDecimal( 1 ), keys.getBigDecimal( 1 ) );
                    assertEquals( stored.getBigDecimal( 1 ), keys.getObject( 1, BigDecimal.class ) );
                    assertFalse( keys.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void failedRawBatchExposesItsCommittedPrefix() throws SQLException {
        String table = "jdbc_raw_batch_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement batch = connection.createStatement();
                Statement ddl = connection.createStatement() ) {
            ddl.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY)" );
            try {
                batch.addBatch( "INSERT INTO " + table + " DEFAULT VALUES; -- committed batch member" );
                batch.addBatch( "INSERT INTO " + table + "_missing DEFAULT VALUES" );
                batch.addBatch( "INSERT INTO " + table + " DEFAULT VALUES" );
                BatchUpdateException failure = assertThrows( BatchUpdateException.class, batch::executeBatch );
                assertBatchPrefix( failure, "42P01", 1L );
                try ( ResultSet keys = batch.getGeneratedKeys() ) {
                    assertSingleKey( keys, 1L );
                }
                try ( Connection observer = DriverManager.getConnection( DB_URL, "pa", "" );
                        Statement read = observer.createStatement();
                        ResultSet rows = read.executeQuery( "SELECT id FROM " + table ) ) {
                    assertSingleKey( rows, 1L );
                }
            } finally {
                ddl.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void failedPreparedBatchExposesItsPendingPrefixUntilRollback() throws SQLException {
        String table = "jdbc_prepared_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id BIGSERIAL PRIMARY KEY, label VARCHAR(3) NOT NULL)" );
            try {
                connection.setAutoCommit( false );
                try ( PreparedStatement batch = connection.prepareStatement(
                        "INSERT INTO " + table + " (label) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                    batch.setString( 1, "one" );
                    batch.addBatch();
                    batch.setString( 1, "longer" );
                    batch.addBatch();
                    BatchUpdateException failure = assertThrows( BatchUpdateException.class, batch::executeLargeBatch );
                    assertEquals( "Char value is too long", failure.getMessage() );
                    assertBatchPrefix( failure, "XX000", 1L );
                    try ( ResultSet keys = batch.getGeneratedKeys() ) {
                        assertSingleKey( keys, 1L );
                    }
                    try ( ResultSet rows = sql.executeQuery( "SELECT id FROM " + table ) ) {
                        assertSingleKey( rows, 1L );
                    }
                }
                connection.rollback();
                connection.setAutoCommit( true );
                try ( ResultSet rows = sql.executeQuery( "SELECT id FROM " + table ) ) {
                    assertFalse( rows.next() );
                }
            } finally {
                if ( !connection.getAutoCommit() ) {
                    connection.rollback();
                    connection.setAutoCommit( true );
                }
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    @Test
    void atomicPreparedBatchFailureDoesNotExposeRolledBackKeys() throws SQLException {
        String table = "jdbc_atomic_" + UUID.randomUUID().toString().replace( "-", "" );
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement sql = connection.createStatement() ) {
            sql.executeUpdate( "CREATE TABLE " + table + " (id BIGINT GENERATED BY DEFAULT AS IDENTITY (START WITH 10 MAXVALUE 10) PRIMARY KEY, label INTEGER)" );
            try ( PreparedStatement batch = connection.prepareStatement(
                    "INSERT INTO " + table + " (label) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
                batch.setInt( 1, 1 );
                batch.addBatch();
                batch.setInt( 1, 2 );
                batch.addBatch();
                BatchUpdateException failure = assertThrows( BatchUpdateException.class, batch::executeBatch );
                assertBatchPrefix( failure, "2200H" );
                assertArrayEquals( new int[0], failure.getUpdateCounts() );
                try ( ResultSet keys = batch.getGeneratedKeys();
                        ResultSet rows = sql.executeQuery( "SELECT id FROM " + table ) ) {
                    assertFalse( keys.next() );
                    assertFalse( rows.next() );
                }
            } finally {
                sql.executeUpdate( "DROP TABLE " + table );
            }
        }
    }


    private static void assertBatchPrefix( BatchUpdateException failure, String state, long... expectedCounts ) {
        assertBatchDiagnostic( failure, state );
        assertArrayEquals( expectedCounts, failure.getLargeUpdateCounts() );
    }


    private static void assertBatchDiagnostic( BatchUpdateException failure, String state ) {
        assertEquals( state, failure.getSQLState() );
        assertEquals( -1, failure.getErrorCode() );
        PrismInterfaceServiceException original = assertInstanceOf( PrismInterfaceServiceException.class, failure.getCause() );
        assertEquals( failure.getSQLState(), original.getSQLState() );
        assertEquals( failure.getErrorCode(), original.getErrorCode() );
    }


    private static void assertSingleKey( ResultSet rows, long expectedKey ) throws SQLException {
        assertTrue( rows.next() );
        assertEquals( expectedKey, rows.getLong( 1 ) );
        assertFalse( rows.next() );
    }


    private static void assertNoCursorPosition( ResultSet keys ) throws SQLException {
        assertFalse( keys.isBeforeFirst() );
        assertFalse( keys.isAfterLast() );
        assertFalse( keys.isFirst() );
        assertFalse( keys.isLast() );
        assertEquals( 0, keys.getRow() );
    }

}

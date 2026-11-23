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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("prism25")
class Prism25IntegrationTest {

    private static final String DB_URL = "jdbc:polypheny://127.0.0.1:" + System.getenv().getOrDefault( "POLYPHENY_TEST_PORT", "20590" );


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
        assertEquals( state, failure.getSQLState() );
        assertEquals( -1, failure.getErrorCode() );
        PrismInterfaceServiceException original = assertInstanceOf( PrismInterfaceServiceException.class, failure.getCause() );
        assertEquals( failure.getSQLState(), original.getSQLState() );
        assertEquals( failure.getErrorCode(), original.getErrorCode() );
        assertArrayEquals( expectedCounts, failure.getLargeUpdateCounts() );
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

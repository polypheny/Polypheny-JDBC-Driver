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
import static org.junit.jupiter.api.Assertions.assertTrue;

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


    private static void assertNoCursorPosition( ResultSet keys ) throws SQLException {
        assertFalse( keys.isBeforeFirst() );
        assertFalse( keys.isAfterLast() );
        assertFalse( keys.isFirst() );
        assertFalse( keys.isLast() );
        assertEquals( 0, keys.getRow() );
    }

}

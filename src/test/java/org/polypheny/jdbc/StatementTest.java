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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatementTest {

    Connection con;


    @BeforeEach
    void createConnection() throws SQLException {
        con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590", "pa", "" );
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
            statement.execute( "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER NOT NULL)" );
        }
    }


    @AfterEach
    void closeConnection() throws SQLException {
        con.close();
    }


    @Test
    void testPrepareStatement() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();
        }
        try ( Statement s = con.createStatement() ) {
            ResultSet resultSet = s.executeQuery( "SELECT id, a FROM t WHERE id = 4" );
            assertTrue( resultSet.next() );
            assertEquals( 4, resultSet.getInt( 1 ) );
            assertEquals( 4, resultSet.getInt( 1 ) );
            assertFalse( resultSet.next() );
        }
    }


    @Test
    void testLargeBatch() throws SQLException {
        try ( Statement s = con.createStatement() ) {
            s.addBatch( "INSERT INTO t(id, a) VALUES (1, 1)" );
            s.addBatch( "INSERT INTO t(id, a) VALUES (2, 2)" );
            s.addBatch( "INSERT INTO t(id, a) VALUES (3, 3)" );
            long[] res = s.executeLargeBatch();
            assertArrayEquals( new long[]{ 1, 1, 1 }, res );
        }
    }

}

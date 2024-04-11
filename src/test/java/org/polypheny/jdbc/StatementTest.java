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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(TestHelper.class)
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
    void testMoreThanOneExecute() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();
            p.setInt( 1, 5 );
            p.setInt( 2, 5 );
            p.execute();
        }
    }


    @Test
    void testStatementSingleExecCleanup() throws SQLException {
        try ( Statement p = con.createStatement() ) {
            p.execute( "INSERT INTO t(id, a) VALUES (1, 4)" );
        }
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
        }
    }


    @Test
    void testStatementMultipleExecCleanup() throws SQLException {
        try ( Statement p = con.createStatement() ) {
            p.execute( "INSERT INTO t(id, a) VALUES (1, 4)" );
            p.execute( "INSERT INTO t(id, a) VALUES (2, 4)" );
        }
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
        }
    }


    @Test
    void testPreparedStatementSingleExecCleanup() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();
        }
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
        }
    }


    @Test
    //@Disabled("Whats the expected behaviour here?")
    void testMultipleStatements() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();

            try ( Statement s = con.createStatement() ) {
                s.execute( "INSERT INTO t(id, a) VALUES (5, 5)" );
                s.execute( "INSERT INTO t(id, a) VALUES (6, 6)" );
            }

            p.setInt( 1, 7 );
            p.setInt( 2, 7 );
            p.execute();
            con.close();
        }
    }


    @Test
    @Disabled("Check in avatica. Does this work there?")
    void testPreparedStatementDualExecCleanup() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();
            p.setInt( 1, 5 );
            p.setInt( 2, 5 );
            p.execute();
        }
        System.out.println( "done" );
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
        }
        System.out.println( "done2" );
    }


    @Test
    @Disabled
    void testPreparedStatementDualExecUpdate() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 4 );
            p.setInt( 2, 4 );
            p.execute();
            p.setInt( 1, 5 );
            p.setInt( 2, 5 );
            p.execute();
        }
        System.out.println( "done" );
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "SELECT * FROM t" );
        }
    }


    @ParameterizedTest()
    @ValueSource(ints = { 99, 100, 101 })
    void testFetch( int n ) throws SQLException {
        // TODO: Switch for and try if testMoreThanOneExecute works
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            for ( int i = 0; i < n; i++ ) {

                p.setInt( 1, i );
                p.setInt( 2, i );
                p.addBatch();
            }
            p.executeBatch();
        }

        try ( Statement s = con.createStatement() ) {
            ResultSet res = s.executeQuery( "SELECT * FROM t" );
            int count = 0;
            while ( res.next() ) {
                // Consume all results
                count++;
            }
            assertEquals( n, count );
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


    @Test
    void testParameterizedLargeBatch() throws SQLException {
        try ( PreparedStatement p = con.prepareStatement( "INSERT INTO t(id, a) VALUES (?, ?)" ) ) {
            p.setInt( 1, 1 );
            p.setInt( 2, 1 );
            p.addBatch();
            p.setInt( 1, 2 );
            p.setInt( 2, 2 );
            p.addBatch();
            long[] res = p.executeLargeBatch();
            assertArrayEquals( new long[]{ 2 }, res );
        }
    }
}

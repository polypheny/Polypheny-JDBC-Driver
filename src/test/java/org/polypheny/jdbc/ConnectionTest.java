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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConnectionTest {

    Connection con;


    @BeforeEach
    void createConnection() throws SQLException {
        con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590", "pa", "" );
    }


    @AfterEach
    void closeConnection() throws SQLException {
        con.close();
    }


    @Test
    void testCommit() throws SQLException {
        con.setAutoCommit( false );
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
            statement.execute( "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER NOT NULL)" );
            statement.execute( "INSERT INTO t(id, a) VALUES (1, 1), (2, 2), (3, 3)" );
            con.commit();
            ResultSet resultSet = statement.executeQuery( "SELECT * FROM t" );
            int count = 0;
            while ( resultSet.next() ) {
                count++;
            }
            assertEquals( count, 3 );
        }
    }


    @Test
    void testRollback() throws SQLException {
        con.setAutoCommit( false );
        try ( Statement statement = con.createStatement() ) {
            statement.execute( "DROP TABLE IF EXISTS t" );
            statement.execute( "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER NOT NULL)" );
            statement.execute( "INSERT INTO t(id, a) VALUES (1, 1), (2, 2), (3, 3)" );
            con.rollback();
            ResultSet resultSet = statement.executeQuery( "SELECT * FROM t" );
            assertFalse( resultSet.next() );
        }
    }



}

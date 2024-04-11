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

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(TestHelper.class)
public class BlockingTest {

    Connection con;


    @Test
    void connectAndDisconnect() throws SQLException {
        con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590", "pa", "" );
        con.close();
    }

    @Test
    void execAndDisconnect() throws SQLException {
        con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590", "pa", "" );
        try ( Statement s = con.createStatement()) {
            s.execute( "DROP TABLE IF EXISTS t" );
            s.execute( "CREATE TABLE t(id INTEGER PRIMARY KEY, a INTEGER NOT NULL)" );
            s.execute( "INSERT INTO t(id, a) VALUES (1, 1), (2, 2), (3, 3)" );
            s.execute("DROP TABLE IF EXISTS t");
        }
        con.close();
    }

    @Test
    void failAndDisconnect() throws SQLException {
        con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590", "pa", "" );
        try ( Statement s = con.createStatement()) {
            s.execute( "DROP TABLE IF EXISTS t" );
            assertThrows(SQLException.class, () -> s.execute( "INSERT INTO t(id, a) VALUES (1, 1), (2, 2), (3, 3)" ));
        }
        con.close();
    }



}

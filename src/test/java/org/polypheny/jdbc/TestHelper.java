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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class TestHelper {

    public static Connection getConnection() {
        final String DB_URL = "jdbc:polypheny://localhost:20590";
        final String USER = "pa";
        final String PASS = "";

        // TODO Class.forName( "org.polypheny.jdbc.PolyphenyDriver" );

        try {
            return DriverManager.getConnection( DB_URL, USER, PASS );

        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    public static void insertTestData() throws SQLException {
        try (
                Connection connection = getConnection();
                Statement statement = connection.createStatement();
        ) {
            statement.execute( "DROP TABLE IF EXISTS customers" );
            statement.execute(
                    "CREATE TABLE customers(\n"
                            + "  id INTEGER PRIMARY KEY,\n"
                            + "  name TEXT NOT NULL,\n"
                            + "  year_joined INTEGER NOT NULL\n"
                            + ")"
            );
            try ( PreparedStatement insert = connection.prepareStatement( "INSERT INTO customers(id, name, year_joined) VALUES (?, ?, ?)" ) ) {
                insert.setInt( 1, 1 );
                insert.setString( 2, "Maria" );
                insert.setInt( 3, 2012 );
                insert.addBatch();
                insert.setInt( 1, 2 );
                insert.setString( 2, "Daniel" );
                insert.setInt( 3, 2020 );
                insert.addBatch();
                insert.setInt( 1, 3 );
                insert.setString( 2, "Peter" );
                insert.setInt( 3, 2001 );
                insert.addBatch();
                insert.setInt( 1, 4 );
                insert.setString( 2, "Anna" );
                insert.setInt( 3, 2001 );
                insert.addBatch();
                insert.setInt( 1, 5 );
                insert.setString( 2, "Thomas" );
                insert.setInt( 3, 2004 );
                insert.addBatch();
                insert.setInt( 1, 6 );
                insert.setString( 2, "Andreas" );
                insert.setInt( 3, 2014 );
                insert.addBatch();
                insert.setInt( 1, 7 );
                insert.setString( 2, "Michael" );
                insert.setInt( 3, 2010 );
                insert.addBatch();
                insert.executeBatch();
            }
        }
    }

}

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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ConnectionTest {

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
    void testCommit() throws SQLException {
        con.setAutoCommit( false );
        try ( Statement statement = con.createStatement() ) {
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
            statement.execute( "INSERT INTO t(id, a) VALUES (1, 1), (2, 2), (3, 3)" );
            con.rollback();
            ResultSet resultSet = statement.executeQuery( "SELECT * FROM t" );
            assertFalse( resultSet.next() );
        }
    }


    @Test
    void testCloseWithOpenStatements() throws SQLException {
        try ( Statement statement = con.createStatement() ) {
            con.close();
            assertTrue( statement.isClosed() );
        }
    }


    @Test
    void testCheckConnection() throws SQLException {
        con.isValid( 0 );
        assertThrows( SQLException.class, () -> con.isValid( -1 ) );
    }


    @Test
    void testClientProperties() throws SQLException {
        Properties info = con.getClientInfo();
        con.setClientInfo( info );
    }


    @Test
    void testMetaDataGetURL() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getURL();
    }


    @Test
    void testMetaDataGetDatabaseProductName() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getDatabaseProductName();
    }


    @Test
    void testMetaDataGetCatalogs() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getCatalogs();
    }


    @Test
    void testMetaDataGetTableTypes() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getTableTypes();
    }


    @Test
    void testMetaDataGetTypeInfo() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getTypeInfo();
    }


    @Test
    void testMetaDataGetColumns() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getColumns( "public", ".*", ".*", ".*" );
    }


    @Test
    void testMetaDataGetStringFunctions() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getStringFunctions();
    }


    @Test
    void testMetaDataGetSystemFunctions() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getSystemFunctions();
    }


    @Test
    void testMetaDataGetTimeDateFunctions() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getTimeDateFunctions();
    }


    @Test
    void testMetaDataGetNumericFunctions() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getNumericFunctions();
    }


    @Test
    void testMetaDataGetSQLKeywords() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        meta.getSQLKeywords();
    }


    @Test
    void testMetaDataGetProceduresNotStrict() throws SQLException {
        try ( Connection con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590?strict=false", "pa", "" ) ) {
            DatabaseMetaData meta = con.getMetaData();
            meta.getProcedures( "public", ".*", ".*" );
        }
    }


    @Test
    @Disabled
        // This test fails due to syntax definitions missing for some of the functions on the server side (operator registry).
    void testMetaDataGetFunctionsNotStrict() throws SQLException {
        try ( Connection con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590?strict=false", "pa", "" ) ) {
            DatabaseMetaData meta = con.getMetaData();
            meta.getFunctions( "public", ".*", ".*" );
        }
    }


    @Test
    void testMetaDataGetSchemasNotStrict() throws SQLException {
        try ( Connection con = DriverManager.getConnection( "jdbc:polypheny://127.0.0.1:20590?strict=false", "pa", "" ) ) {
            DatabaseMetaData meta = con.getMetaData();
            meta.getSchemas( "public", ".*" );
        }
    }


    @Test
    void testUnimplemented() throws SQLException {
        DatabaseMetaData meta = con.getMetaData();
        assertThrows( SQLFeatureNotSupportedException.class, meta::getClientInfoProperties );
        assertThrows( SQLFeatureNotSupportedException.class, () -> meta.getUDTs( "public", ".*", ".*", null ) );
    }


    @Test
    void properShutdown() throws SQLException {
        // This test requires that there is only one active Driver instance (otherwise we will pick up the Thread name of another Connection)
        con.close();
        assertFalse( Thread.getAllStackTraces().keySet().stream().map( Thread::getName ).anyMatch( n -> n.equals( "PrismInterfaceResponseHandler" ) ) );
    }

}

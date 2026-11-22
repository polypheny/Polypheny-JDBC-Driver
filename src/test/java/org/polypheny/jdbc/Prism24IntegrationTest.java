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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("prism24")
class Prism24IntegrationTest {

    private static final String DB_URL = "jdbc:polypheny://127.0.0.1:" + System.getenv().getOrDefault( "POLYPHENY_TEST_PORT", "20590" );
    private static final String MISSING_TABLE = "jdbc_prism24_missing_table";


    @Test
    void statementErrorPreservesDiagnosticsAndAllowsRollback() throws SQLException {
        try ( Connection connection = DriverManager.getConnection( DB_URL, "pa", "" );
                Statement statement = connection.createStatement() ) {
            connection.setAutoCommit( false );
            PrismInterfaceServiceException error = assertThrows( PrismInterfaceServiceException.class,
                    () -> statement.executeUpdate( "DROP TABLE " + MISSING_TABLE ) );
            assertEquals( "42P01", error.getSQLState() );
            assertEquals( -1, error.getErrorCode() );
            assertEquals( "There exists no entity with the name " + MISSING_TABLE + " and 'IF EXISTS' was not specified", error.getMessage() );

            connection.rollback();
            try ( ResultSet result = statement.executeQuery( "SELECT 1" ) ) {
                assertTrue( result.next() );
                assertEquals( 1, result.getInt( 1 ) );
                assertFalse( result.next() );
            }
            connection.commit();
        }
    }


    @Test
    void loginFailurePreservesAuthenticationDiagnostics() {
        PrismInterfaceServiceException error = assertThrows( PrismInterfaceServiceException.class, () -> {
            try ( Connection ignored = DriverManager.getConnection( DB_URL, "pa", "invalid-smoke-test-password" ) ) {
                throw new AssertionError( "Unexpected successful login" );
            }
        } );
        assertEquals( "28000", error.getSQLState() );
        assertEquals( -1, error.getErrorCode() );
        assertEquals( "Wrong username or password", error.getMessage() );
    }

}

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
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.multimodel.DocumentResult;
import org.polypheny.jdbc.multimodel.GraphResult;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.Result.ResultType;
import org.polypheny.jdbc.types.PolyDocument;

public class QueryTest {

    private static final String SQL_LANGUAGE_NAME = "sql";
    private static final String SQL_TEST_QUERY = "SELECT * FROM customers";

    private static final String MQL_LANGUAGE_NAME = "mongo";
    private static final String MQL_TEST_QUERY = "db.customers.find({});";

    private static final String CYPHER_LANGUAGE_NAME = "cypher";
    private static final String CYPHER_TEST_QUERY = "MATCH (c:customers)";


    @BeforeAll
    public static void setup() throws SQLException, ClassNotFoundException {
        TestHelper.insertTestData();
    }


    @Test
    public void thisOneWorks() throws SQLException {
        try (
                Connection connection = TestHelper.getConnection();
                Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery( SQL_TEST_QUERY )
        ) {
            while ( resultSet.next() ) {
                // Process the result set...
            }
        }
    }


    @Test
    public void simpleRelationalTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            Result result = polyStatement.execute( "public", SQL_LANGUAGE_NAME, SQL_TEST_QUERY );
            assertEquals( ResultType.RELATIONAL, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void simpleMqlTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            Result result = polyStatement.execute( "public", MQL_LANGUAGE_NAME, MQL_TEST_QUERY );
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void mqlDataRetrievalTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            Result result = polyStatement.execute( "public", MQL_LANGUAGE_NAME, MQL_TEST_QUERY );
            DocumentResult docs = result.unwrap( DocumentResult.class );
            for ( PolyDocument doc : docs ) {
                // Process the results...
            }
            assertEquals( ResultType.DOCUMENT, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    @Test
    public void simpleCypherTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();
            Result result = polyStatement.execute( "gph", CYPHER_LANGUAGE_NAME, "MATCH (p:Person) RETURN p.age, p.name" );
            RelationalResult relRes = result.unwrap( RelationalResult.class );
            assertEquals( ResultType.RELATIONAL, result.getResultType() );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

}

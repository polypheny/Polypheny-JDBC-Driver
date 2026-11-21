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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.ScalarResult;
import org.polypheny.jdbc.types.TypedValue;

class PolyStatementTest {

    @Test
    void executesSqlBatchesAndBothParameterKindsOnTheSameStatement() throws SQLException, InterruptedException {
        try ( Connection connection = TestHelper.getConnection(); Statement ddl = connection.createStatement() ) {
            ddl.execute( "DROP TABLE IF EXISTS poly_statement_test" );
            ddl.execute( "CREATE TABLE poly_statement_test (id INTEGER PRIMARY KEY)" );
            try ( PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
                assertEquals( Arrays.asList( 1L, 1L ), statement.execute( "public", "sql", Arrays.asList(
                        "INSERT INTO poly_statement_test VALUES (1)",
                        "INSERT INTO poly_statement_test VALUES (2)" ) ) );

                statement.prepareIndexed( "public", "sql", "INSERT INTO poly_statement_test VALUES (?)" );
                assertEquals( 1L, statement.executePrepared( Collections.singletonList( TypedValue.fromInteger( 3 ) ) )
                        .unwrap( ScalarResult.class ).getScalar() );
                assertEquals( Collections.singletonList( 2L ), statement.executePreparedBatch( Arrays.asList(
                        Collections.singletonList( TypedValue.fromInteger( 4 ) ),
                        Collections.singletonList( TypedValue.fromInteger( 5 ) ) ) ) );

                statement.setFetchSize( 1 );
                statement.prepareNamed( "public", "sql",
                        "SELECT id FROM poly_statement_test WHERE id >= :low AND id <= :high ORDER BY id" );
                Map<String, TypedValue> parameters = new LinkedHashMap<>();
                parameters.put( "high", TypedValue.fromInteger( 4 ) );
                parameters.put( "low", TypedValue.fromInteger( 3 ) );
                Iterator<PolyRow> rows = statement.executePrepared( parameters ).unwrap( RelationalResult.class ).iterator();
                assertEquals( 3, rows.next().getValue( 0 ).asInt() );
                assertEquals( 4, rows.next().getValue( 0 ).asInt() );
                assertFalse( rows.hasNext() );
            } finally {
                ddl.execute( "DROP TABLE poly_statement_test" );
            }
        }
    }

}

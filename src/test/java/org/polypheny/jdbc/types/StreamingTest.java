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

package org.polypheny.jdbc.types;

import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.TestHelper;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;

public class StreamingTest {

    private static final String DROP_IF_STATEMENT = "DROP TABLE IF EXISTS file_table";
    private static final String CREATE_STATEMENT = "CREATE TABLE file_table (id INT PRIMARY KEY, data FILE)";
    private static final String INSERT_STATEMENT = "INSERT INTO file_table (id, data) VALUES (?, ?)";
    private static final String QUERY = "SELECT file FROM file_table WHERE id = 0";


    @Test
    public void simpleRelationalTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();

            polyStatement.execute( "public", "sql", DROP_IF_STATEMENT );
            polyStatement.execute( "public", "sql", CREATE_STATEMENT );

            polyStatement.prepare( "public", "sql", INSERT_STATEMENT );
            byte[] expected = createTestData();
            List<TypedValue> parameters = new ArrayList<>( 2 );
            parameters.add( TypedValue.fromInteger( 1 ) );
            parameters.add( TypedValue.fromBytes( expected ) );
            polyStatement.executePrepared( parameters );

            RelationalResult result = polyStatement.execute( "public", "sql", QUERY ).unwrap( RelationalResult.class );
            byte[] received = result.iterator().next().get( "data" ).asBytes();

            Assertions.assertEquals( expected, received );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    private static byte[] createTestData() {
        final int TOTAL_BYTES = 300 * 1024 * 1024;
        final int SIZE = 4;
        final int TOTAL_INTS = TOTAL_BYTES / SIZE;

        byte[] data = new byte[TOTAL_BYTES];
        ByteBuffer buffer = ByteBuffer.wrap( data );
        for ( int i = 0; i < TOTAL_INTS; i++ ) {
            buffer.putInt( i );
        }
        return data;
    }

}

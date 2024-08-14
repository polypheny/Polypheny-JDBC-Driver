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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.TestHelper;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.Result;
import org.polypheny.jdbc.multimodel.ScalarResult;

public class StreamingTest {

    private static final String FILE_DROP_IF_STATEMENT = "DROP TABLE IF EXISTS file_table";
    private static final String FILE_CREATE_STATEMENT = "CREATE TABLE file_table (id INT PRIMARY KEY, data FILE)";
    private static final String FILE_INSERT_STATEMENT = "INSERT INTO file_table (id, data) VALUES (?, ?)";
    private static final String FILE_QUERY = "SELECT data FROM file_table WHERE id = 1";

    private static final String STRING_DROP_IF_STATEMENT = "DROP TABLE IF EXISTS string_table";
    private static final String STRING_CREATE_STATEMENT = "CREATE TABLE string_table (id INT PRIMARY KEY, data VARCHAR)";
    private static final String STRING_INSERT_STATEMENT = "INSERT INTO string_table (id, data) VALUES (?, ?)";
    private static final String STRING_QUERY = "SELECT data FROM string_table WHERE id = 1";


    @Test
    public void simpleFileStreamingTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();

            polyStatement.execute( "public", "sql", FILE_DROP_IF_STATEMENT );
            polyStatement.execute( "public", "sql", FILE_CREATE_STATEMENT );

            polyStatement.prepare( "public", "sql", FILE_INSERT_STATEMENT );
            byte[] expected = createTestData();

            List<TypedValue> parameters = new ArrayList<>( 2 );
            parameters.add( TypedValue.fromInteger( 1 ) );
            parameters.add( TypedValue.fromBytes( expected ) );
            Result result = polyStatement.executePrepared( parameters );
            result.unwrap( ScalarResult.class );

            RelationalResult result2 = polyStatement.execute( "public", "sql", FILE_QUERY ).unwrap( RelationalResult.class );
            InputStream bis = result2.iterator().next().get( "data" ).asBlob().getBinaryStream();
            byte[] received = collectStream( bis );

            Assertions.assertArrayEquals( expected, received );
        } catch ( SQLException | IOException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void simpleFileStreamingTest2() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();

            polyStatement.execute( "public", "sql", FILE_DROP_IF_STATEMENT );
            polyStatement.execute( "public", "sql", FILE_CREATE_STATEMENT );

            polyStatement.prepare( "public", "sql", FILE_INSERT_STATEMENT );
            byte[] expected = createTestData();

            List<TypedValue> parameters = new ArrayList<>( 2 );
            parameters.add( TypedValue.fromInteger( 1 ) );
            parameters.add( TypedValue.fromBlob( new ByteArrayInputStream( expected ) ) );
            Result result = polyStatement.executePrepared( parameters );
            result.unwrap( ScalarResult.class );

            RelationalResult result2 = polyStatement.execute( "public", "sql", FILE_QUERY ).unwrap( RelationalResult.class );
            InputStream bis = result2.iterator().next().get( "data" ).asBlob().getBinaryStream();
            byte[] received = collectStream( bis );

            Assertions.assertArrayEquals( expected, received );
        } catch ( SQLException | IOException e ) {
            throw new RuntimeException( e );
        }
    }


    @Test
    public void simpleStringStreamingTest() {
        try ( Connection connection = TestHelper.getConnection() ) {
            if ( !connection.isWrapperFor( PolyConnection.class ) ) {
                fail( "Driver must support unwrapping to PolyphenyConnection" );
            }
            PolyStatement polyStatement = connection.unwrap( PolyConnection.class ).createPolyStatement();

            polyStatement.execute( "public", "sql", STRING_DROP_IF_STATEMENT );
            polyStatement.execute( "public", "sql", STRING_CREATE_STATEMENT );

            polyStatement.prepare( "public", "sql", STRING_INSERT_STATEMENT );
            String expected = createTestStringData();

            List<TypedValue> parameters = new ArrayList<>( 2 );
            parameters.add( TypedValue.fromInteger( 1 ) );
            parameters.add( TypedValue.fromString( expected ) );
            Result result = polyStatement.executePrepared( parameters );
            result.unwrap( ScalarResult.class );

            RelationalResult result2 = polyStatement.execute( "public", "sql", STRING_QUERY ).unwrap( RelationalResult.class );
            String received = result2.iterator().next().get( "data" ).asString();

            Assertions.assertEquals( expected, received );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    private byte[] collectStream( InputStream inputStream ) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int frameLength;
        byte[] frame = new byte[300 * 1024 * 1024];
        while ( (frameLength = inputStream.read( frame, 0, frame.length )) != -1 ) {
            buffer.write( frame, 0, frameLength );
        }
        buffer.flush();
        return buffer.toByteArray();
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


    private String createTestStringData() {
        char[] data = new char[300 * 1024 * 1024];
        Arrays.fill( data, 'A' );
        return new String( data );
    }

}

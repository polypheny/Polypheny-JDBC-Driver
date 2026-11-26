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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.prism.Namespace;

class MetadataResultSetTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void metadataResultsCloseWithTheirConnection( boolean abort ) throws SQLException {
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost:20590?strict=false" );
        PrismInterfaceClient client = mock( PrismInterfaceClient.class );
        PolyphenyDatabaseMetadata metadata = new PolyphenyDatabaseMetadata( client, target );
        try ( PolyConnection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ), metadata ) ) {
            when( client.searchNamespaces( null, null, connection.getNetworkTimeout() ) )
                    .thenReturn( Collections.singletonList( Namespace.newBuilder().setNamespaceName( "public" ).build() ) );
            try ( ResultSet remote = metadata.getSchemas(); ResultSet local = metadata.getProcedureColumns( null, null, null, null ) ) {
                assertTrue( remote.next() );
                assertEquals( "public", remote.getString( "TABLE_SCHEM" ) );
                assertFalse( local.isClosed() );
                local.setFetchSize( 7 );
                assertEquals( 7, local.getFetchSize() );
                assertEquals( 0, remote.getFetchSize() );
                assertNoPosition( local );
                assertFalse( local.next() );
                assertNoPosition( local );

                if ( abort ) {
                    connection.abortAfterIncompleteRequest();
                } else {
                    connection.close();
                }

                assertTrue( remote.isClosed() );
                assertTrue( local.isClosed() );
                assertThrows( SQLException.class, () -> remote.getString( "TABLE_SCHEM" ) );
                assertThrows( SQLException.class, local::next );
                assertEquals( "08003", assertThrows( SQLException.class,
                        () -> metadata.getProcedureColumns( null, null, null, null ) ).getSQLState() );
            }
        }
    }


    private static void assertNoPosition( ResultSet empty ) throws SQLException {
        assertFalse( empty.isBeforeFirst() );
        assertFalse( empty.isAfterLast() );
        assertFalse( empty.isFirst() );
        assertFalse( empty.isLast() );
        assertEquals( 0, empty.getRow() );
    }

}

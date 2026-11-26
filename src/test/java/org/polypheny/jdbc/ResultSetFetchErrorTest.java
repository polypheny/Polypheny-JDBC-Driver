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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.prism.Frame;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;

class ResultSetFetchErrorTest {

    private final PrismInterfaceClient client = mock( PrismInterfaceClient.class );
    private final PrismInterfaceServiceException failure = new PrismInterfaceServiceException( "Could not fetch rows.", "XX000", 0 );


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void nextPreservesServerExceptionFromPrefetch( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( resultSetType ) ) {
            assertTrue( result.next() );
            assertSame( failure, assertThrows( PrismInterfaceServiceException.class, result::next ) );
            assertSame( failure, assertThrows( PrismInterfaceServiceException.class, result::next ) );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void lastPropagatesFailureFromExistingPrefetch() throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( ResultSet.TYPE_SCROLL_INSENSITIVE ) ) {
            assertTrue( result.next() );
            assertSame( failure, assertThrows( PrismInterfaceServiceException.class, result::last ) );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void fetchAllPropagatesFailureFromExistingPrefetch() throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenThrow( failure );
        try ( PolyphenyResultSet result = newResultSet( ResultSet.TYPE_FORWARD_ONLY ) ) {
            assertTrue( result.next() );
            assertSame( failure, assertThrows( PrismInterfaceServiceException.class, result::fetchAll ) );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void lastIncludesEveryFetchedPage() throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenReturn( frame( false ), frame( true ) );
        try ( ResultSet result = newResultSet( ResultSet.TYPE_SCROLL_INSENSITIVE ) ) {
            assertTrue( result.next() );
            assertTrue( result.last() );
            assertEquals( 3, result.getRow() );
        }
    }


    private PolyphenyResultSet newResultSet( int resultSetType ) throws SQLException {
        PolyphenyStatement statement = mock( PolyphenyStatement.class );
        PolyConnection connection = mock( PolyConnection.class );
        when( statement.getConnection() ).thenReturn( connection );
        when( statement.getClient() ).thenReturn( client );
        when( statement.getStatementId() ).thenReturn( 7 );
        when( connection.getNetworkTimeout() ).thenReturn( 2000 );
        PolyphenyResultSetProperties properties = PolyphenyResultSetProperties.forMetaResultSet();
        properties.setResultSetType( resultSetType );
        properties.setStatementFetchSize( 1 );
        return new PolyphenyResultSet( statement, frame( false ), properties );
    }


    private static Frame frame( boolean last ) {
        return Frame.newBuilder().setIsLast( last )
                .setRelationalFrame( RelationalFrame.newBuilder().addRows( Row.getDefaultInstance() ) ).build();
    }

}

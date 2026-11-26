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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementResult;

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


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void afterLastThenPreviousFindsTheLastAvailableRow( boolean capped ) throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenReturn( frame( false ), frame( !capped ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( ResultSet.TYPE_SCROLL_INSENSITIVE, 1, capped ? 3 : 0 ) ) {
            result.afterLast();
            assertTrue( result.isAfterLast() );
            assertEquals( 0, result.getRow() );
            assertTrue( result.previous() );
            assertEquals( 3, result.getRow() );
            assertTrue( result.isLast() );
            assertTrue( result.previous() );
            assertEquals( 2, result.getRow() );
            result.afterLast();
            assertFalse( result.next() );
        }
        verify( client, times( 2 ) ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void afterLastKeepsTheCurrentRowWhenAFetchFails() throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( ResultSet.TYPE_SCROLL_INSENSITIVE ) ) {
            assertTrue( result.next() );
            assertSame( failure, assertThrows( PrismInterfaceServiceException.class, result::afterLast ) );
            assertEquals( 1, result.getRow() );
            assertFalse( result.isAfterLast() );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void afterLastRejectsForwardOnlyResultsBeforeFetching() throws SQLException {
        try ( ResultSet result = newResultSet( ResultSet.TYPE_FORWARD_ONLY ) ) {
            assertThrows( SQLException.class, result::afterLast );
            assertTrue( result.isBeforeFirst() );
        }
        verify( client, never() ).fetchResult( anyInt(), anyInt(), anyInt() );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void aLargerCursorFetchSizeStillRespectsMaxRows( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 4, 2000 ) ).thenReturn( frame( false, 4 ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( resultSetType, 1, 3 ) ) {
            result.setFetchSize( 4 );
            assertTrue( result.next() );
            assertTrue( result.isFirst() );
            assertFalse( result.isLast() );
            assertTrue( result.next() );
            assertTrue( result.next() );
            assertTrue( result.isLast() );
            assertFalse( result.next() );
            assertTrue( result.isAfterLast() );
            assertFalse( result.isFirst() );
            assertEquals( 0, result.getRow() );
            assertEquals( 1, statement.getFetchSize() );
        }
        verify( client ).fetchResult( 7, 4, 2000 );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void reachingMaxRowsInTheFirstFrameDoesNotFetch( int resultSetType ) throws SQLException {
        try ( PolyphenyResultSet result = newResultSet( resultSetType, 1, 1, frame( false, 2 ) ) ) {
            assertTrue( result.isBeforeFirst() );
            assertFalse( result.isAfterLast() );
            assertEquals( 0, result.getRow() );
            result.fetchAll();
            assertTrue( result.next() );
            assertTrue( result.isFirst() );
            assertTrue( result.isLast() );
            assertEquals( 1, result.getRow() );
            assertFalse( result.next() );
            assertTrue( result.isAfterLast() );
            assertFalse( result.isFirst() );
            assertFalse( result.isLast() );
            assertEquals( 0, result.getRow() );
        }
        verify( client, never() ).fetchResult( anyInt(), anyInt(), anyInt() );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void emptyOrdinaryResultsHaveNoCursorPosition( int resultSetType ) throws SQLException {
        try ( ResultSet result = newResultSet( resultSetType, 1, 0, frame( true, 0 ) ) ) {
            assertNoPosition( result );
            assertFalse( result.next() );
            assertNoPosition( result );
            if ( resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE ) {
                result.beforeFirst();
                assertNoPosition( result );
                assertFalse( result.first() );
                assertFalse( result.last() );
                assertFalse( result.absolute( 0 ) );
                result.afterLast();
                assertNoPosition( result );
            }
        }
        verify( client, never() ).fetchResult( anyInt(), anyInt(), anyInt() );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void anEmptyNonterminalFrameStillAllowsFetching( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenReturn( frame( true ) );
        try ( ResultSet result = newResultSet( resultSetType, 1, 0, frame( false, 0 ) ) ) {
            assertTrue( result.isBeforeFirst() );
            assertFalse( result.isAfterLast() );
            assertTrue( result.next() );
            assertTrue( result.isFirst() );
            assertTrue( result.isLast() );
            assertEquals( 1, result.getRow() );
            assertFalse( result.next() );
            assertTrue( result.isAfterLast() );
            assertEquals( 0, result.getRow() );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void scrollsBackToTheFirstRowOfACappedResult() throws SQLException {
        try ( ResultSet result = newResultSet( ResultSet.TYPE_SCROLL_INSENSITIVE, 1, 1, frame( false, 2 ) ) ) {
            assertTrue( result.absolute( -1 ) );
            assertEquals( 1, result.getRow() );
            assertTrue( result.isLast() );
            assertFalse( result.absolute( 0 ) );
            assertTrue( result.isBeforeFirst() );
            assertEquals( 0, result.getRow() );
            result.afterLast();
            assertTrue( result.isAfterLast() );
            assertEquals( 0, result.getRow() );
            assertTrue( result.previous() );
            assertEquals( 1, result.getRow() );
        }
        verify( client, never() ).fetchResult( anyInt(), anyInt(), anyInt() );
    }


    private static void assertNoPosition( ResultSet result ) throws SQLException {
        assertFalse( result.isBeforeFirst() );
        assertFalse( result.isAfterLast() );
        assertFalse( result.isFirst() );
        assertFalse( result.isLast() );
        assertEquals( 0, result.getRow() );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void aSmallerCursorFetchSizeAllowsAShortFinalPage( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) ).thenReturn( frame( true ) );
        try ( ResultSet result = newResultSet( resultSetType, 6, 4 ) ) {
            result.setFetchSize( 1 );
            assertTrue( result.next() );
            assertTrue( result.next() );
            assertFalse( result.next() );
            assertEquals( 6, statement.getFetchSize() );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @Test
    void zeroStatementFetchSizeUsesTheDefaultOnPreparedExecution() throws SQLException {
        RpcService rpc = mock( RpcService.class );
        when( rpc.prepareIndexedStatement( any(), anyInt() ) ).thenReturn( PreparedStatementSignature.newBuilder().setStatementId( 7 ).build() );
        when( rpc.executeIndexedStatement( any(), any( RequestDeadline.class ) ) ).thenAnswer( invocation -> {
            ExecuteIndexedStatementRequest request = invocation.getArgument( 0 );
            assertTrue( request.hasFetchSize() );
            assertEquals( PropertyUtils.getDEFAULT_FETCH_SIZE(), request.getFetchSize() );
            return StatementResult.newBuilder().setFrame( frame( true ) ).build();
        } );
        try ( PolyConnection connection = newConnection( new PrismInterfaceClient( rpc ) );
                PreparedStatement query = connection.prepareStatement( "SELECT 1" ) ) {
            query.setFetchSize( 3 );
            query.setFetchSize( 0 );
            try ( ResultSet result = query.executeQuery() ) {
                assertTrue( result.next() );
                assertEquals( PropertyUtils.getDEFAULT_FETCH_SIZE(), result.getFetchSize() );
            }
        }
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void zeroCursorFetchSizeRestoresTheCapturedStatementHint( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 0, 2000 ) ).thenThrow( failure );
        when( client.fetchResult( 7, 1, 2000 ) ).thenReturn( frame( false ) ).thenThrow( failure );
        try ( ResultSet result = newResultSet( resultSetType, 1, 2 ) ) {
            result.setFetchSize( 4 );
            statement.setFetchSize( 8 );
            result.setFetchSize( 0 );
            if ( resultSetType == ResultSet.TYPE_SCROLL_INSENSITIVE ) {
                result.afterLast();
                assertTrue( result.previous() );
            } else {
                assertTrue( result.next() );
                assertTrue( result.next() );
            }
            assertEquals( 2, result.getRow() );
            assertFalse( result.next() );
            assertEquals( 1, result.getFetchSize() );
            assertEquals( 8, statement.getFetchSize() );
        }
        verify( client ).fetchResult( 7, 1, 2000 );
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void fetchUsesTheCurrentNetworkTimeoutAndClosesTheConnectionIfTheReplyIsMissing( int resultSetType ) throws SQLException {
        TimeoutException timeout = new TimeoutException();
        when( client.fetchResult( 7, 1, 75 ) ).thenThrow( new PrismInterfaceServiceException( timeout ) );
        try ( PolyphenyResultSet result = newResultSet( resultSetType ) ) {
            assertFalse( result.isClosed() );
            PolyConnection connection = statement.getPolyConnection();
            connection.setNetworkTimeout( Runnable::run, 75 );

            SQLException failure = assertThrows( SQLException.class, result::fetchAll );

            assertEquals( "08003", failure.getSQLState() );
            assertSame( timeout, failure.getCause() );
            assertTrue( connection.isClosed() );
            assertTrue( statement.isClosed() );
            assertTrue( result.isClosed() );
            verify( client ).abort();
        }
    }


    @ParameterizedTest
    @ValueSource(ints = { ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE })
    void aFetchThatTimedOutBeforeSendingCanBeRetried( int resultSetType ) throws SQLException {
        when( client.fetchResult( 7, 1, 2000 ) )
                .thenThrow( new PrismInterfaceServiceException( new RequestDeadline.BeforeSendTimeout( 2000 ) ) )
                .thenReturn( frame( true ) );
        try ( ResultSet result = newResultSet( resultSetType ) ) {
            assertTrue( result.next() );
            assertInstanceOf( RequestDeadline.BeforeSendTimeout.class, assertThrows( SQLException.class, result::next ).getCause() );
            assertFalse( statement.getPolyConnection().isClosed() );
            assertTrue( result.next() );
            assertFalse( result.next() );
            verify( client, times( 2 ) ).fetchResult( 7, 1, 2000 );
        }
    }


    private PolyphenyStatement statement;


    private PolyphenyResultSet newResultSet( int resultSetType ) throws SQLException {
        return newResultSet( resultSetType, 1, 0 );
    }


    private PolyphenyResultSet newResultSet( int resultSetType, int statementFetchSize, int maxRows ) throws SQLException {
        return newResultSet( resultSetType, statementFetchSize, maxRows, frame( false ) );
    }


    private PolyphenyResultSet newResultSet( int resultSetType, int statementFetchSize, int maxRows, Frame firstFrame ) throws SQLException {
        PolyConnection connection = newConnection( client );
        statement = (PolyphenyStatement) connection.createStatement( resultSetType, ResultSet.CONCUR_READ_ONLY );
        statement.statementId = 7;
        statement.setFetchSize( statementFetchSize );
        statement.setMaxRows( maxRows );
        return (PolyphenyResultSet) statement.publishResult( firstFrame, -1 );
    }


    private static PolyConnection newConnection( PrismInterfaceClient client ) throws SQLException {
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( new ConnectionString( "jdbc:polypheny://localhost:20590" ), client );
        PolyConnection connection = new PolyConnection( properties, mock( PolyphenyDatabaseMetadata.class ) );
        connection.setNetworkTimeout( Runnable::run, 2000 );
        return connection;
    }


    private static Frame frame( boolean last ) {
        return frame( last, 1 );
    }


    private static Frame frame( boolean last, int rowCount ) {
        RelationalFrame.Builder rows = RelationalFrame.newBuilder();
        for ( int row = 0; row < rowCount; row++ ) {
            rows.addRows( Row.getDefaultInstance() );
        }
        return Frame.newBuilder().setIsLast( last ).setRelationalFrame( rows ).build();
    }

}

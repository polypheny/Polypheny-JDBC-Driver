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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Response;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.TypeMeta;


class GeneratedKeysTest {

    private static final String INSERT = "INSERT INTO items DEFAULT VALUES";


    @Test
    void executesUnpreparedWithEachSelectionAndResetsKeysOnReuse() throws Exception {
        RpcService rpc = mock( RpcService.class );
        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            assertTrue( connection.getMetaData().supportsGetGeneratedKeys() );
            assertFalse( statement.getGeneratedKeys().next() );

            stubUnprepared( rpc, GeneratedKeys.ALL, response( 1, frame( "id", 41 ) ) );
            assertEquals( 1, statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            assertNull( statement.getResultSet() );
            ResultSet keys = statement.getGeneratedKeys();
            assertSame( statement, keys.getStatement() );
            assertTrue( keys.getMetaData().isAutoIncrement( 1 ) );
            assertTrue( keys.next() );
            assertEquals( 41, keys.getLong( "id" ) );
            assertFalse( keys.next() );
            assertFalse( statement.getMoreResults() );
            assertFalse( keys.isClosed() );

            GeneratedKeysRequest named = GeneratedKeys.requested( new String[]{ "code" } );
            stubUnprepared( rpc, named, response( 1, frame( "code", 42 ) ) );
            assertFalse( statement.execute( INSERT, new String[]{ "code" } ) );
            assertTrue( keys.isClosed() );
            try ( ResultSet namedKeys = statement.getGeneratedKeys() ) {
                assertTrue( namedKeys.next() );
                assertEquals( 42, namedKeys.getLong( "code" ) );
            }

            GeneratedKeysRequest indexed = GeneratedKeys.requested( new int[]{ 2 } );
            stubUnprepared( rpc, indexed, response( 1, frame( "code", 43 ) ) );
            assertEquals( 1, statement.executeLargeUpdate( INSERT, new int[]{ 2 } ) );
            try ( ResultSet indexedKeys = statement.getGeneratedKeys() ) {
                assertTrue( indexedKeys.next() );
                assertEquals( 43, indexedKeys.getLong( 1 ) );
            }

            stubUnprepared( rpc, null, response( 1, null ) );
            assertEquals( 1, statement.executeUpdate( INSERT, Statement.NO_GENERATED_KEYS ) );
            assertFalse( statement.getGeneratedKeys().next() );
            assertThrows( SQLException.class, () -> statement.executeUpdate( INSERT, 0 ) );
            assertThrows( SQLException.class, () -> statement.executeUpdate( INSERT, new int[]{ 0 } ) );
            assertThrows( SQLException.class, () -> statement.executeUpdate( INSERT, new String[]{ null } ) );
            verify( rpc, never() ).closeResult( any(), anyInt() );
        }
    }


    @Test
    void preparedKeyRequestsKeepTheBindingAndRejectAnUnsetParameter() throws Exception {
        RpcService rpc = mock( RpcService.class );
        when( rpc.prepareIndexedStatement( any(), anyInt() ) ).thenReturn( PreparedStatementSignature.newBuilder()
                .setStatementId( 14 )
                .addParameterMetas( ParameterMeta.newBuilder().setTypeName( "INTEGER" ) )
                .build() );
        when( rpc.executeIndexedStatement( any(), anyInt() ) ).thenAnswer( invocation -> {
            ExecuteIndexedStatementRequest request = invocation.getArgument( 0 );
            assertEquals( Collections.singletonList( TypedValue.fromInteger( 8 ).serialize() ), request.getParameters().getParametersList() );
            return response( 1, frame( "id", 10 ) );
        } );

        try ( PolyConnection connection = connection( rpc, true );
                PreparedStatement statement = connection.prepareStatement( "INSERT INTO items (label) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setInt( 1, 8 );
            assertEquals( 1, statement.executeUpdate() );
            assertFalse( statement.execute() );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( 10, keys.getLong( 1 ) );
            }
            statement.clearParameters();
            assertEquals( "07001", assertThrows( SQLException.class, statement::executeUpdate ).getSQLState() );
            assertEquals( "07001", assertThrows( SQLException.class, statement::addBatch ).getSQLState() );
            doAnswer( invocation -> {
                ExecuteIndexedStatementRequest request = invocation.getArgument( 0 );
                assertEquals( Collections.singletonList( TypedValue.fromNull().serialize() ), request.getParameters().getParametersList() );
                return response( 1, frame( "id", 11 ) );
            } ).when( rpc ).executeIndexedStatement( any(), anyInt() );
            statement.setNull( 1, Types.INTEGER );
            assertEquals( 1, statement.executeUpdate() );
            verify( rpc, times( 3 ) ).executeIndexedStatement( any(), anyInt() );
        }
    }


    @Test
    void preparedCharacterStreamReplacesRetainedBinding() throws Exception {
        RpcService rpc = mock( RpcService.class );
        when( rpc.prepareIndexedStatement( any(), anyInt() ) ).thenReturn( PreparedStatementSignature.newBuilder()
                .setStatementId( 14 )
                .addParameterMetas( ParameterMeta.newBuilder().setTypeName( "VARCHAR" ) )
                .build() );
        when( rpc.executeIndexedStatement( any(), anyInt() ) ).thenReturn( response( 1, frame( "id", 10 ) ) );

        try ( PolyConnection connection = connection( rpc, true );
                PreparedStatement statement = connection.prepareStatement( "INSERT INTO items (label) VALUES (?)", Statement.RETURN_GENERATED_KEYS ) ) {
            statement.setString( 1, "first" );
            assertEquals( 1, statement.executeUpdate() );
            assertThrows( SQLException.class, () -> statement.setCharacterStream( 1, new StringReader( "short" ), 6L ) );
            assertThrows( SQLException.class, () -> statement.setCharacterStream( 1, new StringReader( "second" ), -1 ) );
            statement.setCharacterStream( 1, new StringReader( "second trailing characters" ), 6 );
            assertEquals( 1, statement.executeUpdate() );
        }

        ArgumentCaptor<ExecuteIndexedStatementRequest> requests = ArgumentCaptor.forClass( ExecuteIndexedStatementRequest.class );
        verify( rpc, times( 2 ) ).executeIndexedStatement( requests.capture(), anyInt() );
        assertEquals( "first", requests.getAllValues().get( 0 ).getParameters().getParameters( 0 ).getString().getString() );
        assertEquals( "second", requests.getAllValues().get( 1 ).getParameters().getParameters( 0 ).getString().getString() );
    }


    @Test
    void preparedBatchReturnsKeysInBindingOrderWithoutClosingRemoteResult() throws Exception {
        RpcService rpc = mock( RpcService.class );
        GeneratedKeysRequest requested = GeneratedKeys.requested( new String[]{ "id" } );
        when( rpc.prepareIndexedStatement( any(), anyInt() ) ).thenAnswer( invocation -> {
            PrepareStatementRequest request = invocation.getArgument( 0 );
            assertEquals( requested, request.getGeneratedKeys() );
            return PreparedStatementSignature.newBuilder().setStatementId( 14 ).build();
        } );
        when( rpc.executeIndexedStatement( any(), anyInt() ) ).thenReturn( response( 1, frame( "id", 10 ) ) );
        when( rpc.executeIndexedStatementBatch( any(), anyLong() ) ).thenAnswer( invocation -> {
            ExecuteIndexedStatementBatchRequest request = invocation.getArgument( 0 );
            assertEquals( 14, request.getStatementId() );
            assertEquals( 2, request.getParametersCount() );
            return StatementBatchResponse.newBuilder()
                    .addScalars( 1 ).addScalars( 1 )
                    .addGeneratedKeys( batchKeys( 1, frame( "id", 12 ) ) )
                    .addGeneratedKeys( batchKeys( 0, frame( "id", 11 ) ) )
                    .build();
        } );

        try ( PolyConnection connection = connection( rpc, true );
                PreparedStatement statement = connection.prepareStatement( INSERT, new String[]{ "id" } ) ) {
            assertEquals( 1, statement.executeUpdate() );
            ResultSet previous = statement.getGeneratedKeys();
            assertTrue( previous.next() );
            assertEquals( 10, previous.getLong( 1 ) );

            statement.addBatch();
            statement.addBatch();
            assertArrayEquals( new int[]{ 1, 1 }, statement.executeBatch() );
            assertTrue( previous.isClosed() );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( 11, keys.getLong( 1 ) );
                assertTrue( keys.next() );
                assertEquals( 12, keys.getLong( 1 ) );
                assertFalse( keys.next() );
            }

            statement.addBatch();
            statement.clearBatch();
            assertEquals( 0, statement.executeBatch().length );
            assertFalse( statement.getGeneratedKeys().next() );
            verify( rpc, never() ).closeResult( any(), anyInt() );
        }
    }


    @Test
    void rawBatchCombinesKeysFromDifferentTables() throws Exception {
        RpcService rpc = mock( RpcService.class );
        when( rpc.executeUnparameterizedStatementBatch( any(), anyLong(), any() ) ).thenAnswer( invocation -> {
            ExecuteUnparameterizedStatementBatchRequest request = invocation.getArgument( 0 );
            List<ExecuteUnparameterizedStatementRequest> statements = request.getStatementsList();
            assertEquals( 3, statements.size() );
            statements.forEach( item -> assertEquals( GeneratedKeys.ALL, item.getGeneratedKeys() ) );
            IntConsumer acceptBatchId = invocation.getArgument( 2 );
            acceptBatchId.accept( 22 );
            return StatementBatchResponse.newBuilder()
                    .setBatchId( 22 ).addAllScalars( Arrays.asList( 1L, 1L, 1L ) )
                    .addGeneratedKeys( batchKeys( 2, frame( "other_id", 81 ) ) )
                    .addGeneratedKeys( batchKeys( 0, frame( "id", 41 ) ) ).build();
        } );

        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            statement.addBatch( INSERT );
            statement.addBatch( "UPDATE items SET label = 'new'" );
            statement.addBatch( "INSERT INTO other DEFAULT VALUES" );
            assertArrayEquals( new long[]{ 1, 1, 1 }, statement.executeLargeBatch() );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertEquals( 2, keys.getMetaData().getColumnCount() );
                assertTrue( keys.next() );
                assertEquals( 41, keys.getLong( "id" ) );
                assertNull( keys.getObject( "other_id" ) );
                assertTrue( keys.next() );
                assertNull( keys.getObject( "id" ) );
                assertEquals( 81, keys.getLong( "other_id" ) );
                assertFalse( keys.next() );
            }
        }
    }


    @Test
    void keyAndQueryResultsAreIndependentUntilCloseOnCompletion() throws Exception {
        RpcService rpc = mock( RpcService.class );
        StatementResult response = StatementResult.newBuilder()
                .setFrame( frame( "code", 72 ) )
                .setGeneratedKeys( frame( "id", 42 ) )
                .build();
        stubUnprepared( rpc, GeneratedKeys.ALL, response );

        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            statement.closeOnCompletion();
            // Exercise client cursor handling with a synthetic dual-frame response.
            assertTrue( statement.execute( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            try ( ResultSet queryResult = statement.getResultSet() ) {
                assertTrue( queryResult.next() );
                assertEquals( 72, queryResult.getLong( 1 ) );
            }
            assertFalse( statement.isClosed() );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( 42, keys.getLong( 1 ) );
            }
            assertTrue( statement.isClosed() );
            verify( rpc ).closeResult( any(), anyInt() );
        }
    }


    @Test
    void serverWithoutCapabilityDoesNotReceiveGeneratedKeyRequests() throws Exception {
        RpcService rpc = mock( RpcService.class );
        stubUnprepared( rpc, null, response( 1, null ) );
        try ( PolyConnection connection = connection( rpc, false );
                Statement statement = connection.createStatement() ) {
            assertFalse( connection.getMetaData().supportsGetGeneratedKeys() );
            assertThrows( SQLFeatureNotSupportedException.class, statement::getGeneratedKeys );
            assertThrows( SQLFeatureNotSupportedException.class, () -> statement.execute( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            assertThrows( SQLFeatureNotSupportedException.class, () -> connection.prepareStatement( INSERT, new int[]{ 1 } ) );
            assertEquals( 1, statement.executeUpdate( INSERT, Statement.NO_GENERATED_KEYS ) );
        }
    }


    @Test
    void limitsGeneratedKeyRowsWithoutChangingUpdateCount() throws Exception {
        RpcService rpc = mock( RpcService.class );
        stubUnprepared( rpc, GeneratedKeys.ALL, response( 2, frame( "id", 41, 42 ) ) );
        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            statement.setMaxRows( 1 );
            assertEquals( 2, statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            assertEquals( 2, statement.getUpdateCount() );
            statement.setMaxRows( 0 );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( 41, keys.getLong( 1 ) );
                assertFalse( keys.next() );
            }
            assertEquals( 2, statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS ) );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertTrue( keys.next() );
                assertEquals( 42, keys.getLong( 1 ) );
                assertFalse( keys.next() );
            }
        }
    }


    @Test
    void generatedKeysUseTheFieldLimitFromTheirExecution() throws Exception {
        RpcService rpc = mock( RpcService.class );
        Frame projectedKey = Frame.newBuilder().setIsLast( true )
                .setRelationalFrame( RelationalFrame.newBuilder()
                        .addColumnMeta( ColumnMeta.newBuilder().setColumnName( "label" ).setColumnLabel( "label" )
                                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.VARCHAR ) ) )
                        .addRows( Row.newBuilder().addValues( TypedValue.fromString( "abcdef" ).serialize() ) ) )
                .build();
        String sql = "INSERT INTO items (label) VALUES ('abcdef')";
        String[] selectedKeys = { "label" };
        stubUnprepared( rpc, GeneratedKeys.requested( selectedKeys ), response( 1, projectedKey ) );
        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            statement.setMaxFieldSize( 2 );
            assertEquals( 1, statement.executeUpdate( sql, selectedKeys ) );
            statement.setMaxFieldSize( 0 );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( "ab", keys.getString( 1 ) );
            }

            assertEquals( 1, statement.executeUpdate( sql, selectedKeys ) );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( "abcdef", keys.getString( 1 ) );
            }
        }
    }


    @Test
    void generatedKeysLimitFieldsByBytesWithoutSplittingCodePoints() throws Exception {
        RpcService rpc = mock( RpcService.class );
        byte[] binary = { 0, 1, 2, (byte) 0xFF, 4 };
        Frame projectedKeys = Frame.newBuilder().setIsLast( true )
                .setRelationalFrame( RelationalFrame.newBuilder()
                        .addColumnMeta( ColumnMeta.newBuilder().setColumnName( "label" ).setColumnLabel( "label" )
                                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.VARCHAR ) ) )
                        .addColumnMeta( ColumnMeta.newBuilder().setColumnName( "payload" ).setColumnLabel( "payload" )
                                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.VARBINARY ) ) )
                        .addRows( Row.newBuilder().addValues( TypedValue.fromString( "é🙂z" ).serialize() )
                                .addValues( TypedValue.fromBytes( binary ).serialize() ) ) )
                .build();
        String[] selection = { "label", "payload" };
        stubUnprepared( rpc, GeneratedKeys.requested( selection ), response( 1, projectedKeys ) );
        try ( PolyConnection connection = connection( rpc, true ); Statement statement = connection.createStatement() ) {
            statement.setMaxFieldSize( 4 );
            assertEquals( 1, statement.executeUpdate( INSERT, selection ) );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( "é", keys.getString( 1 ) );
                assertArrayEquals( new byte[]{ 0, 1, 2, (byte) 0xFF }, keys.getBytes( 2 ) );
            }

            statement.setMaxFieldSize( 6 );
            assertEquals( 1, statement.executeUpdate( INSERT, selection ) );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertTrue( keys.next() );
                assertEquals( "é🙂", keys.getString( 1 ) );
                assertArrayEquals( binary, keys.getBytes( 2 ) );
            }
        }
    }


    @Test
    void exposesTypesAndSupportsTypedGetObjectForNamedKeys() throws Exception {
        RpcService rpc = mock( RpcService.class );
        BigDecimal amount = new BigDecimal( "-123.4567" );
        long id = 9_007_199_254_740_993L;
        String[] selection = { "amount", "id", "label" };
        Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.newBuilder()
                .addColumnMeta( ColumnMeta.newBuilder().setColumnIndex( 0 ).setColumnName( "amount" ).setColumnLabel( "amount" )
                        .setPrecision( 12 ).setScale( 4 ).setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.DECIMAL ) ) )
                .addColumnMeta( ColumnMeta.newBuilder().setColumnIndex( 1 ).setColumnName( "id" ).setColumnLabel( "id" )
                        .setIsAutoIncrement( true ).setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.BIGINT ) ) )
                .addColumnMeta( ColumnMeta.newBuilder().setColumnIndex( 2 ).setColumnName( "label" ).setColumnLabel( "label" )
                        .setIsNullable( true ).setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.VARCHAR ) ) )
                .addRows( Row.newBuilder().addValues( TypedValue.fromBigDecimal( amount ).serialize() )
                        .addValues( TypedValue.fromLong( id ).serialize() ).addValues( TypedValue.fromNull().serialize() ) ) ).build();
        stubUnprepared( rpc, GeneratedKeys.requested( selection ), response( 1, frame ) );

        try ( PolyConnection connection = connection( rpc, true ); Statement statement = connection.createStatement() ) {
            assertEquals( 1, statement.executeUpdate( INSERT, selection ) );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                ResultSetMetaData metadata = keys.getMetaData();
                assertEquals( Types.DECIMAL, metadata.getColumnType( 1 ) );
                assertEquals( 12, metadata.getPrecision( 1 ) );
                assertEquals( 4, metadata.getScale( 1 ) );
                assertEquals( BigDecimal.class.getName(), metadata.getColumnClassName( 1 ) );
                assertTrue( metadata.isSigned( 1 ) );
                assertEquals( Types.BIGINT, metadata.getColumnType( 2 ) );
                assertEquals( 0, metadata.getScale( 2 ) );
                assertEquals( Long.class.getName(), metadata.getColumnClassName( 2 ) );
                assertTrue( metadata.isSigned( 2 ) );
                assertTrue( metadata.isAutoIncrement( 2 ) );
                assertEquals( String.class.getName(), metadata.getColumnClassName( 3 ) );
                assertFalse( metadata.isSigned( 3 ) );

                assertTrue( keys.next() );
                assertEquals( amount, keys.getObject( "amount", BigDecimal.class ) );
                assertEquals( Long.valueOf( id ), keys.getObject( 2, Long.class ) );
                assertFalse( keys.wasNull() );
                assertNull( keys.getObject( "label", String.class ) );
                assertTrue( keys.wasNull() );
                assertThrows( SQLException.class, () -> keys.getObject( 2, byte[].class ) );
                assertThrows( SQLException.class, () -> keys.getObject( 3, (Class<?>) null ) );
            }
        }
    }


    @Test
    void emptyGeneratedKeysHaveNoCursorPosition() throws Exception {
        RpcService rpc = mock( RpcService.class );
        stubUnprepared( rpc, GeneratedKeys.ALL, response( 1, frame( "id" ) ) );
        try ( PolyConnection connection = connection( rpc, true );
                Statement statement = connection.createStatement() ) {
            statement.executeUpdate( INSERT, Statement.RETURN_GENERATED_KEYS );
            try ( ResultSet keys = statement.getGeneratedKeys() ) {
                assertEquals( 1, keys.getMetaData().getColumnCount() );
                assertNoCursorPosition( keys );
                assertFalse( keys.next() );
                assertNoCursorPosition( keys );
            }
        }
    }


    private static void assertNoCursorPosition( ResultSet keys ) throws SQLException {
        assertFalse( keys.isBeforeFirst() );
        assertFalse( keys.isAfterLast() );
        assertFalse( keys.isFirst() );
        assertFalse( keys.isLast() );
        assertEquals( 0, keys.getRow() );
    }


    private static PolyConnection connection( RpcService rpc, boolean capability ) throws SQLException {
        ConnectionResponse.Builder response = ConnectionResponse.newBuilder().setIsCompatible( true );
        if ( capability ) {
            response.setSupportsGeneratedKeys( true );
        }
        when( rpc.connect( any(), anyInt() ) ).thenReturn( response.build() );
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost:20590/public" );
        PolyphenyConnectionProperties properties = new PolyphenyConnectionProperties( target, client );
        client.register( properties, properties.getNetworkTimeout() );
        return new PolyConnection( properties, new PolyphenyDatabaseMetadata( client, target ) );
    }


    private static void stubUnprepared( RpcService rpc, GeneratedKeysRequest requestKeys, StatementResult response ) throws Exception {
        doAnswer( invocation -> {
            ExecuteUnparameterizedStatementRequest request = invocation.getArgument( 0 );
            if ( requestKeys == null ) {
                assertFalse( request.hasGeneratedKeys() );
            } else {
                assertEquals( requestKeys, request.getGeneratedKeys() );
            }
            CallbackQueue<StatementResponse> callback = invocation.getArgument( 1 );
            callback.onNext( Response.newBuilder().setStatementResponse( StatementResponse.newBuilder().setStatementId( 7 ).setResult( response ) ).build() );
            callback.onCompleted();
            return null;
        } ).when( rpc ).executeUnparameterizedStatement( any(), any() );
    }


    private static StatementResult response( long count, Frame keys ) {
        StatementResult.Builder result = StatementResult.newBuilder().setScalar( count );
        if ( keys != null ) {
            result.setGeneratedKeys( keys );
        }
        return result.build();
    }


    private static BatchGeneratedKeys batchKeys( int index, Frame frame ) {
        return BatchGeneratedKeys.newBuilder().setStatementIndex( index ).setKeys( frame ).build();
    }


    private static Frame frame( String name, long... values ) throws SQLException {
        RelationalFrame.Builder frame = RelationalFrame.newBuilder()
                .addColumnMeta( ColumnMeta.newBuilder().setColumnName( name ).setColumnLabel( name ).setIsAutoIncrement( true )
                        .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.BIGINT ) ) );
        for ( long value : values ) {
            frame.addRows( Row.newBuilder().addValues( TypedValue.fromLong( value ).serialize() ) );
        }
        return Frame.newBuilder().setIsLast( true ).setRelationalFrame( frame ).build();
    }

}

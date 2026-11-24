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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.meta.MetaResultSetBuilder;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.Column;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.TypeMeta;


class GeneratedKeysResultTest {

    @Test
    void usesTheDeclaredSqlTypeForFloatingPointObjects() throws SQLException {
        RelationalFrame frame = RelationalFrame.newBuilder()
                .addColumnMeta( column( "floating", "items", "public", ProtoPolyType.FLOAT ).toBuilder().clearIsAutoIncrement() )
                .addColumnMeta( column( "real_value", "items", "public", ProtoPolyType.REAL ).toBuilder().setColumnIndex( 1 ).clearIsAutoIncrement() )
                .addRows( Row.newBuilder().addValues( TypedValue.fromFloat( 1.25f ).serialize() )
                        .addValues( TypedValue.fromFloat( 1.5f ).serialize() ) )
                .addRows( Row.newBuilder().addValues( TypedValue.fromNull().serialize() )
                        .addValues( TypedValue.fromNull().serialize() ) )
                .build();
        try ( ResultSet keys = resultSet( Frame.newBuilder().setIsLast( true ).setRelationalFrame( frame ).build() ) ) {
            ResultSetMetaData metadata = keys.getMetaData();
            assertEquals( Types.FLOAT, metadata.getColumnType( 1 ) );
            assertEquals( Double.class.getName(), metadata.getColumnClassName( 1 ) );
            assertEquals( Types.REAL, metadata.getColumnType( 2 ) );
            assertEquals( Float.class.getName(), metadata.getColumnClassName( 2 ) );

            assertTrue( keys.next() );
            assertEquals( Double.valueOf( 1.25d ), keys.getObject( "floating" ) );
            assertEquals( Double.valueOf( 1.25d ), keys.getObject( 1, Double.class ) );
            assertEquals( Float.valueOf( 1.5f ), keys.getObject( "real_value" ) );
            assertEquals( Float.valueOf( 1.5f ), keys.getObject( 2, Float.class ) );
            assertFalse( keys.wasNull() );

            assertTrue( keys.next() );
            assertNull( keys.getObject( 1 ) );
            assertTrue( keys.wasNull() );
            assertNull( keys.getObject( 1, Double.class ) );
            assertTrue( keys.wasNull() );
            assertNull( keys.getObject( 2, Float.class ) );
            assertTrue( keys.wasNull() );
        }
    }


    @Test
    void rejectsFractionalAndOutOfRangeIntegralKeyValues() throws SQLException {
        Frame frame = frame( column( "id", "items", "public", ProtoPolyType.BIGINT ),
                TypedValue.fromBigDecimal( new BigDecimal( "12.5" ) ),
                TypedValue.fromBigDecimal( new BigDecimal( "9223372036854775808" ) ) );
        try ( ResultSet keys = resultSet( frame ) ) {
            assertTrue( keys.next() );
            SQLException fractional = assertThrows( SQLException.class, () -> keys.getLong( 1 ) );
            assertEquals( "22003", fractional.getSQLState() );
            assertTrue( keys.next() );
            SQLException outOfRange = assertThrows( SQLException.class, () -> keys.getObject( 1, Long.class ) );
            assertEquals( "22003", outOfRange.getSQLState() );
        }
    }


    @Test
    void combinesIntegerIdentityTypesInStatementOrderWithoutLosingValuesOrSchema() throws SQLException {
        Frame small = frame( column( "id", "small_items", "public", ProtoPolyType.SMALLINT ), TypedValue.fromShort( Short.MIN_VALUE ) );
        Frame integer = frame( column( "ID", "items", "", ProtoPolyType.INTEGER ).toBuilder().setNamespace( "public" ).build(),
                TypedValue.fromInteger( Integer.MIN_VALUE ) );
        Frame big = frame( column( "id", "big_items", "public", ProtoPolyType.BIGINT ),
                TypedValue.fromLong( Long.MAX_VALUE ), TypedValue.fromLong( Long.MIN_VALUE ) );
        Frame combined = GeneratedKeys.combine( Arrays.asList( batchKeys( 1, integer ), batchKeys( 2, big ), batchKeys( 0, small ) ) );

        try ( ResultSet keys = resultSet( combined ) ) {
            ResultSetMetaData metadata = keys.getMetaData();
            assertEquals( 1, metadata.getColumnCount() );
            assertEquals( Types.BIGINT, metadata.getColumnType( 1 ) );
            assertEquals( "public", metadata.getSchemaName( 1 ) );
            assertEquals( "", metadata.getTableName( 1 ) );
            assertTrue( metadata.isAutoIncrement( 1 ) );
            for ( long expected : new long[]{ Short.MIN_VALUE, Integer.MIN_VALUE, Long.MAX_VALUE, Long.MIN_VALUE } ) {
                assertTrue( keys.next() );
                assertEquals( Long.valueOf( expected ), keys.getObject( "id" ) );
            }
            assertFalse( keys.next() );
        }
    }


    @Test
    void discardsAmbiguousSchemaAndTableAcrossNamespaces() throws SQLException {
        Frame source = frame( column( "id", "items", "public", ProtoPolyType.INTEGER ), TypedValue.fromInteger( 1 ) );
        Frame other = frame( column( "id", "items", "inventory", ProtoPolyType.INTEGER ), TypedValue.fromInteger( 2 ) );
        try ( ResultSet keys = resultSet( GeneratedKeys.combine( Arrays.asList( batchKeys( 0, source ), batchKeys( 1, other ) ) ) ) ) {
            assertEquals( "", keys.getMetaData().getSchemaName( 1 ) );
            assertEquals( "", keys.getMetaData().getTableName( 1 ) );
        }
    }


    @Test
    void supportsBothSchemaMetadataFields() throws SQLException {
        ColumnMeta schemaName = column( "id", "items", "server_schema", ProtoPolyType.INTEGER );
        try ( ResultSet keys = resultSet( frame( schemaName ) ) ) {
            assertEquals( "server_schema", keys.getMetaData().getSchemaName( 1 ) );
            assertEquals( "items", keys.getMetaData().getTableName( 1 ) );
        }
        try ( ResultSet keys = resultSet( frame( schemaName.toBuilder().setNamespace( "legacy_schema" ).build() ) ) ) {
            assertEquals( "legacy_schema", keys.getMetaData().getSchemaName( 1 ) );
        }
    }


    @Test
    void exposesPresentOrUnknownAutoIncrementMetadata() throws SQLException {
        ColumnMeta identity = column( "id", "items", "public", ProtoPolyType.INTEGER );
        RelationalFrame columns = RelationalFrame.newBuilder()
                .addColumnMeta( identity )
                .addColumnMeta( identity.toBuilder().setColumnIndex( 1 ).setIsAutoIncrement( false ) )
                .addColumnMeta( identity.toBuilder().setColumnIndex( 2 ).clearIsAutoIncrement() )
                .build();
        try ( ResultSet keys = resultSet( Frame.newBuilder().setIsLast( true ).setRelationalFrame( columns ).build() ) ) {
            ResultSetMetaData metadata = keys.getMetaData();
            assertTrue( metadata.isAutoIncrement( 1 ) );
            assertFalse( metadata.isAutoIncrement( 2 ) );
            assertFalse( metadata.isAutoIncrement( 3 ) );
        }
        Column.Builder metadataColumn = Column.newBuilder().setNamespaceName( "public" ).setTableName( "items" ).setTypeName( "INTEGER" );
        try ( ResultSet metadata = MetaResultSetBuilder.buildFromColumns( Arrays.asList(
                metadataColumn.setColumnIndex( 1 ).setColumnName( "id" ).setIsAutoIncrement( true ).build(),
                metadataColumn.setColumnIndex( 2 ).setColumnName( "quantity" ).setIsAutoIncrement( false ).build(),
                metadataColumn.setColumnIndex( 3 ).setColumnName( "unknown" ).clearIsAutoIncrement().build() ) ) ) {
            for ( String expected : new String[]{ "YES", "NO", "" } ) {
                assertTrue( metadata.next() );
                assertEquals( expected, metadata.getString( "IS_AUTOINCREMENT" ) );
            }
            assertFalse( metadata.next() );
        }
    }


    private static ResultSet resultSet( Frame frame ) throws SQLException {
        return new PolyphenyResultSet( mock( PolyphenyStatement.class ), GeneratedKeys.requireComplete( frame ), PolyphenyResultSetProperties.forMetaResultSet() );
    }


    private static Frame frame( ColumnMeta column, TypedValue... values ) throws SQLException {
        RelationalFrame.Builder relational = RelationalFrame.newBuilder().addColumnMeta( column );
        for ( TypedValue value : values ) {
            relational.addRows( Row.newBuilder().addValues( value.serialize() ) );
        }
        return Frame.newBuilder().setIsLast( true ).setRelationalFrame( relational ).build();
    }


    private static ColumnMeta column( String name, String table, String schema, ProtoPolyType type ) {
        return ColumnMeta.newBuilder()
                .setColumnName( name )
                .setColumnLabel( name )
                .setEntityName( table )
                .setSchemaName( schema )
                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( type ) )
                .setIsAutoIncrement( true )
                .build();
    }


    private static BatchGeneratedKeys batchKeys( int statementIndex, Frame keys ) {
        return BatchGeneratedKeys.newBuilder().setStatementIndex( statementIndex ).setKeys( keys ).build();
    }

}

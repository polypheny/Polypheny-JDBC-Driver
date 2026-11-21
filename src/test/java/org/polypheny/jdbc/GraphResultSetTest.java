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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.polypheny.jdbc.properties.PolyphenyResultSetProperties;
import org.polypheny.jdbc.types.PolyEdge;
import org.polypheny.jdbc.types.PolyGraph;
import org.polypheny.jdbc.types.PolyNode;
import org.polypheny.jdbc.types.PolyPath;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoGraph;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoNull;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.TypeMeta;

class GraphResultSetTest {

    @ParameterizedTest
    @MethodSource( "graphValues" )
    void readsGraphValuesAndMetadata( ProtoPolyType type, ProtoValue value, Class<?> javaType ) throws SQLException {
        ColumnMeta column = ColumnMeta.newBuilder()
                .setColumnIndex( 0 )
                .setColumnName( "graph_value" )
                .setColumnLabel( "projection" )
                .setIsNullable( true )
                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( type ) )
                .build();
        Frame frame = Frame.newBuilder()
                .setIsLast( true )
                .setRelationalFrame( RelationalFrame.newBuilder()
                        .addColumnMeta( column )
                        .addRows( Row.newBuilder().addValues( value ) )
                        .addRows( Row.newBuilder().addValues( ProtoValue.newBuilder().setNull( ProtoNull.getDefaultInstance() ) ) ) )
                .build();
        PolyphenyStatement statement = mock( PolyphenyStatement.class );
        when( statement.getConnection() ).thenReturn( mock( PolyConnection.class ) );
        PolyphenyResultSetProperties properties = PolyphenyResultSetProperties.forMetaResultSet();
        properties.setResultSetType( ResultSet.TYPE_FORWARD_ONLY );

        try ( ResultSet rows = new PolyphenyResultSet( statement, frame, properties ) ) {
            ResultSetMetaData metadata = rows.getMetaData();
            assertEquals( Types.JAVA_OBJECT, metadata.getColumnType( 1 ) );
            assertEquals( type.name(), metadata.getColumnTypeName( 1 ) );
            assertEquals( javaType.getName(), metadata.getColumnClassName( 1 ) );
            assertTrue( rows.next() );
            assertInstanceOf( javaType, rows.getObject( 1 ) );
            assertInstanceOf( javaType, rows.getObject( "PROJECTION", javaType ) );
            assertFalse( rows.wasNull() );
            assertTrue( rows.next() );
            assertNull( rows.getObject( "projection", javaType ) );
            assertTrue( rows.wasNull() );
            assertFalse( rows.next() );
        }
    }


    static Stream<Arguments> graphValues() {
        ProtoNode node = ProtoNode.newBuilder().setId( "n" ).build();
        ProtoEdge edge = ProtoEdge.newBuilder().setId( "e" ).setLeft( "n" ).setRight( "n" ).build();
        return Stream.of(
                Arguments.of( ProtoPolyType.NODE, ProtoValue.newBuilder().setNode( node ).build(), PolyNode.class ),
                Arguments.of( ProtoPolyType.EDGE, ProtoValue.newBuilder().setEdge( edge ).build(), PolyEdge.class ),
                Arguments.of( ProtoPolyType.PATH, ProtoValue.newBuilder()
                        .setPath( ProtoPath.newBuilder().addNodes( node ).addEdges( edge ).addNodes( node ) ).build(), PolyPath.class ),
                Arguments.of( ProtoPolyType.GRAPH, ProtoValue.newBuilder()
                        .setGraph( ProtoGraph.newBuilder().addNodes( node ).addEdges( edge ) ).build(), PolyGraph.class ) );
    }

}

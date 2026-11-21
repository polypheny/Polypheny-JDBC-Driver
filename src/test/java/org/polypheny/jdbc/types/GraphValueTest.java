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

package org.polypheny.jdbc.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.types.PolyEdge.EdgeDirection;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoEdge;
import org.polypheny.prism.ProtoGraph;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoPath;
import org.polypheny.prism.ProtoValue;

class GraphValueTest {

    @Test
    void preservesPathTraversalAndEdgeOrientation() throws SQLException {
        ProtoNode a = ProtoNode.newBuilder().setId( "a" ).build();
        ProtoNode b = ProtoNode.newBuilder().setId( "b" ).build();
        ProtoEdge outgoing = ProtoEdge.newBuilder()
                .setId( "out" )
                .setLeft( "a" )
                .setRight( "b" )
                .setDirection( ProtoEdge.Direction.LEFT_TO_RIGHT )
                .build();
        ProtoEdge incoming = outgoing.toBuilder().setId( "in" ).setDirection( ProtoEdge.Direction.RIGHT_TO_LEFT ).build();
        TypedValue value = new TypedValue( ProtoValue.newBuilder()
                .setPath( ProtoPath.newBuilder().addNodes( b ).addNodes( a ).addNodes( b ).addEdges( outgoing ).addEdges( incoming ) )
                .build() );

        PolyPath path = value.asPath();
        assertEquals( Arrays.asList( "b", "a", "b" ), path.getNodes().stream().map( PolyNode::getId ).collect( Collectors.toList() ) );
        assertEquals( Arrays.asList( "out", "in" ), path.getEdges().stream().map( PolyEdge::getId ).collect( Collectors.toList() ) );
        assertEquals( "a", path.getEdges().get( 0 ).getLeft() );
        assertEquals( "b", path.getEdges().get( 0 ).getRight() );
        assertEquals( EdgeDirection.LEFT_TO_RIGHT, path.getEdges().get( 0 ).getDirection() );
        assertEquals( EdgeDirection.RIGHT_TO_LEFT, path.getEdges().get( 1 ).getDirection() );
        assertThrows( SQLException.class, value::asNode );
        assertThrows( SQLException.class, value::asDocument );
    }


    @Test
    void readsGraphsAndTheirPropertiesInsideListsAndDocuments() throws SQLException {
        ProtoNode node = ProtoNode.newBuilder()
                .setId( "n" )
                .setName( "person" )
                .addLabels( "Person" )
                .putProperties( "name", TypedValue.fromString( "Alice" ).serialize() )
                .build();
        ProtoNode isolated = ProtoNode.newBuilder().setId( "isolated" ).build();
        ProtoEdge edge = ProtoEdge.newBuilder().setLeft( "n" ).setRight( "n" ).setDirectionValue( 99 ).build();
        ProtoGraph graph = ProtoGraph.newBuilder().addNodes( node ).addNodes( isolated ).addEdges( edge ).build();
        ProtoValue value = ProtoValue.newBuilder().setDocument( ProtoDocument.newBuilder()
                .putEntries( "values", ProtoValue.newBuilder().setList( ProtoList.newBuilder()
                        .addValues( ProtoValue.newBuilder().setGraph( graph ) )
                        .addValues( ProtoValue.newBuilder().setNode( node ) ) ).build() ) ).build();

        Array list = new TypedValue( value ).asDocument().get( "values" ).asArray();
        assertEquals( Types.JAVA_OBJECT, list.getBaseType() );
        Object[] values = (Object[]) list.getArray();
        PolyGraph decoded = assertInstanceOf( PolyGraph.class, values[0] );
        assertEquals( "isolated", decoded.getNodes().get( 1 ).getId() );
        assertEquals( EdgeDirection.UNRECOGNIZED, decoded.getEdges().get( 0 ).getDirection() );
        PolyNode person = assertInstanceOf( PolyNode.class, values[1] );
        assertEquals( "person", person.getName() );
        assertEquals( Arrays.asList( "Person" ), person.getLabels() );
        assertEquals( "Alice", person.get( "name" ).asString() );
    }

}

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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.multimodel.GraphResult;
import org.polypheny.jdbc.multimodel.PolyRow;
import org.polypheny.jdbc.multimodel.PolyStatement;
import org.polypheny.jdbc.multimodel.RelationalResult;
import org.polypheny.jdbc.multimodel.Result.ResultType;
import org.polypheny.jdbc.types.PolyEdge;
import org.polypheny.jdbc.types.PolyEdge.EdgeDirection;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.types.PolyNode;
import org.polypheny.jdbc.types.PolyPath;

@Tag("graph")
class GraphQueryTest {

    private static final String NAMESPACE = "jdbc_graph_test";
    private static final String CYPHER = "cypher";
    private static final List<String> PERSON_NAMES = Arrays.asList( "Alice", "Bob", "Charlie" );
    private static final String PATH_MATCH = "MATCH p=(:Person {name: 'Alice'})-[:KNOWS*1..3]->(:Person {name: 'Charlie'})";


    @BeforeAll
    static void createGraph() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                Statement ddl = connection.createStatement();
                PolyStatement cypher = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            ddl.execute( "DROP NAMESPACE IF EXISTS " + NAMESPACE );
            ddl.execute( "CREATE GRAPH NAMESPACE " + NAMESPACE );
            assertEquals( ResultType.SCALAR, cypher.execute( NAMESPACE, CYPHER,
                    "CREATE (:Person {id: 1, name: 'Alice'})-[:KNOWS {since: 2020}]->"
                            + "(:Person {id: 2, name: 'Bob'})-[:KNOWS {since: 2021}]->"
                            + "(:Person {id: 3, name: 'Charlie'})" ).getResultType() );
        }
    }


    @AfterAll
    static void dropGraph() throws SQLException {
        try ( Connection connection = TestHelper.getConnection(); Statement ddl = connection.createStatement() ) {
            ddl.execute( "DROP NAMESPACE IF EXISTS " + NAMESPACE );
        }
    }


    @Test
    void iteratesGraphFramesAcrossPages() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            statement.setFetchSize( 1 );
            GraphResult result = statement.execute( NAMESPACE, CYPHER, "MATCH (n:Person) RETURN n ORDER BY n.id" ).unwrap( GraphResult.class );
            Iterator<PolyGraphElement> nodes = result.iterator();
            for ( String name : PERSON_NAMES ) {
                assertTrue( nodes.hasNext() );
                assertNodeName( name, assertInstanceOf( PolyNode.class, nodes.next() ) );
            }
            assertFalse( nodes.hasNext() );

            GraphResult empty = statement.execute( NAMESPACE, CYPHER,
                    "MATCH (n:Person {name: 'Missing'}) RETURN n" ).unwrap( GraphResult.class );
            assertFalse( empty.iterator().hasNext() );
        }
    }


    @Test
    void resolvesGraphAndScalarColumnsAcrossPagesByAlias() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            statement.setFetchSize( 1 );
            RelationalResult result = statement.execute( NAMESPACE, CYPHER,
                    "MATCH (n:Person) RETURN n.name AS name, n AS person ORDER BY name" ).unwrap( RelationalResult.class );
            assertEquals( 2, result.getMetadata().getColumnCount() );
            assertEquals( "person", result.getMetadata().getColumnMeta( 1 ).getColumnLabel() );
            assertEquals( "NODE", result.getMetadata().getColumnMeta( 1 ).getProtocolTypeName() );

            Iterator<PolyRow> rows = result.iterator();
            for ( String name : PERSON_NAMES ) {
                assertTrue( rows.hasNext() );
                PolyRow row = rows.next();
                assertEquals( name, row.get( "name" ).asString() );
                assertNodeName( name, row.get( "person" ).asNode() );
            }
            assertFalse( rows.hasNext() );
        }
    }


    @Test
    void preservesEdgeEndpointsInMixedRows() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            RelationalResult result = statement.execute( NAMESPACE, CYPHER,
                    "MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person) "
                            + "RETURN a AS source, r AS relationship, b AS target, b.name AS name" ).unwrap( RelationalResult.class );
            assertEquals( "EDGE", result.getMetadata().getColumnMeta( 1 ).getProtocolTypeName() );
            Iterator<PolyRow> rows = result.iterator();
            PolyRow row = rows.next();
            assertEquals( "Bob", row.get( "name" ).asString() );
            assertEdge( row.get( "source" ).asNode(), row.get( "relationship" ).asEdge(), row.get( "target" ).asNode(), 2020 );
            assertFalse( rows.hasNext() );
        }
    }


    @Test
    void keepsGraphColumnMetadataForEmptyMixedResults() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            RelationalResult result = statement.execute( NAMESPACE, CYPHER,
                    "MATCH (n:Person {name: 'Missing'}) RETURN n AS person, n.name AS name" ).unwrap( RelationalResult.class );
            assertEquals( 2, result.getMetadata().getColumnCount() );
            assertEquals( 0, result.getMetadata().getColumnIndexFromLabel( "person" ) );
            assertEquals( "NODE", result.getMetadata().getColumnMeta( 0 ).getProtocolTypeName() );
            assertFalse( result.iterator().hasNext() );
        }
    }


    @Test
    void readsAnOrderedPathFromBothResultFormats() throws SQLException {
        try ( Connection connection = TestHelper.getConnection();
                PolyStatement statement = connection.unwrap( PolyConnection.class ).createPolyStatement() ) {
            GraphResult graph = statement.execute( NAMESPACE, CYPHER, PATH_MATCH + " RETURN p" ).unwrap( GraphResult.class );
            Iterator<PolyGraphElement> elements = graph.iterator();
            PolyNode alice = assertInstanceOf( PolyNode.class, elements.next() );
            PolyEdge first = assertInstanceOf( PolyEdge.class, elements.next() );
            PolyNode bob = assertInstanceOf( PolyNode.class, elements.next() );
            PolyEdge second = assertInstanceOf( PolyEdge.class, elements.next() );
            PolyNode charlie = assertInstanceOf( PolyNode.class, elements.next() );
            assertFalse( elements.hasNext() );
            assertNodeName( "Alice", alice );
            assertNodeName( "Bob", bob );
            assertNodeName( "Charlie", charlie );
            assertEdge( alice, first, bob, 2020 );
            assertEdge( bob, second, charlie, 2021 );

            RelationalResult mixed = statement.execute( NAMESPACE, CYPHER,
                    PATH_MATCH + " RETURN p AS route, 'trip' AS title" ).unwrap( RelationalResult.class );
            assertEquals( "PATH", mixed.getMetadata().getColumnMeta( 0 ).getProtocolTypeName() );
            Iterator<PolyRow> rows = mixed.iterator();
            PolyRow row = rows.next();
            assertFalse( rows.hasNext() );
            assertEquals( "trip", row.get( "title" ).asString() );
            PolyPath path = row.get( "route" ).asPath();
            assertEquals( 3, path.getNodes().size() );
            assertEquals( 2, path.getEdges().size() );
            for ( int i = 0; i < PERSON_NAMES.size(); i++ ) {
                assertNodeName( PERSON_NAMES.get( i ), path.getNodes().get( i ) );
            }
            assertEdge( path.getNodes().get( 0 ), path.getEdges().get( 0 ), path.getNodes().get( 1 ), 2020 );
            assertEdge( path.getNodes().get( 1 ), path.getEdges().get( 1 ), path.getNodes().get( 2 ), 2021 );
        }
    }


    private static void assertNodeName( String name, PolyNode node ) throws SQLException {
        assertTrue( node.getLabels().contains( "Person" ) );
        assertEquals( name, node.get( "name" ).asString() );
    }


    private static void assertEdge( PolyNode left, PolyEdge edge, PolyNode right, long since ) throws SQLException {
        assertEquals( left.getId(), edge.getLeft() );
        assertEquals( right.getId(), edge.getRight() );
        assertTrue( edge.getLabels().contains( "KNOWS" ) );
        assertEquals( EdgeDirection.LEFT_TO_RIGHT, edge.getDirection() );
        assertEquals( Long.toString( since ), edge.get( "since" ).asString() );
    }

}

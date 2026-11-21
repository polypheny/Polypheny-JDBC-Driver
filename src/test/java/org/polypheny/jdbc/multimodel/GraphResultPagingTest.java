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

package org.polypheny.jdbc.multimodel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.types.PolyGraphElement;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GraphElement;
import org.polypheny.prism.GraphFrame;
import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

class GraphResultPagingTest {

    private static final int FETCH_SIZE = 2;
    private static final int TIMEOUT = 31;

    private PrismInterfaceClient client;
    private PolyStatement statement;


    @BeforeEach
    void setUp() throws Exception {
        client = mock( PrismInterfaceClient.class );
        PolyConnection connection = mock( PolyConnection.class );
        when( connection.getPrismInterfaceClient() ).thenReturn( client );
        when( connection.getTimeout() ).thenReturn( TIMEOUT );
        statement = new PolyStatement( connection );
        statement.setFetchSize( FETCH_SIZE );
    }


    @Test
    void iteratesAcrossEmptyGraphPages() throws Exception {
        stubResult( "MATCH (n) RETURN n", 1, graphFrame( false ) );
        when( client.fetchResult( 1, FETCH_SIZE, TIMEOUT ) ).thenReturn( graphFrame( false ), graphFrame( true, "node" ) );

        GraphResult result = statement.execute( "graph", "cypher", "MATCH (n) RETURN n" ).unwrap( GraphResult.class );
        Iterator<PolyGraphElement> elements = result.iterator();
        assertTrue( elements.hasNext() );
        assertEquals( "node", elements.next().getId() );
        assertFalse( elements.hasNext() );
    }


    @Test
    void anOldResultCannotFetchFromANewExecution() throws Exception {
        stubResult( "MATCH (n) RETURN n", 1, graphFrame( false, "first" ) );
        stubResult( "MATCH (m) RETURN m", 2, graphFrame( true, "second" ) );

        GraphResult firstResult = statement.execute( "graph", "cypher", "MATCH (n) RETURN n" ).unwrap( GraphResult.class );
        Iterator<PolyGraphElement> first = firstResult.iterator();
        statement.execute( "graph", "cypher", "MATCH (m) RETURN m" );

        assertEquals( "first", first.next().getId() );
        assertThrows( IllegalStateException.class, first::hasNext );
        verify( client ).closeStatement( 1, TIMEOUT );
        verify( client, never() ).fetchResult( 2, FETCH_SIZE, TIMEOUT );
    }


    private void stubResult( String query, int statementId, Frame frame ) throws Exception {
        doAnswer( invocation -> {
            CallbackQueue<StatementResponse> callback = invocation.getArgument( 4 );
            callback.onNext( Response.newBuilder()
                    .setStatementResponse( StatementResponse.newBuilder()
                            .setStatementId( statementId )
                            .setResult( StatementResult.newBuilder().setFrame( frame ) ) )
                    .build() );
            callback.onCompleted();
            return null;
        } ).when( client ).executeUnparameterizedStatement( eq( "graph" ), eq( "cypher" ), eq( query ), eq( FETCH_SIZE ), any(), eq( TIMEOUT ) );
    }


    private static Frame graphFrame( boolean last, String... nodes ) {
        GraphFrame.Builder graph = GraphFrame.newBuilder();
        for ( String node : nodes ) {
            graph.addElement( GraphElement.newBuilder().setNode( ProtoNode.newBuilder().setId( node ) ) );
        }
        return Frame.newBuilder().setIsLast( last ).setGraphFrame( graph ).build();
    }

}

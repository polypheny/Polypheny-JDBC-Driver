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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.TypeMeta;

class ResultSetTransportClosureTest {

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void bufferedResultsCloseAsSoonAsTheTransportCloses( boolean generatedKeys ) throws SQLException {
        PrismInterfaceClient client = mock( PrismInterfaceClient.class );
        when( client.supportsGeneratedKeys() ).thenReturn( true );
        ConnectionString target = new ConnectionString( "jdbc:polypheny://localhost:20590" );
        try ( PolyConnection connection = new PolyConnection( new PolyphenyConnectionProperties( target, client ),
                new PolyphenyDatabaseMetadata( client, target ) );
                PolyphenyStatement statement = (PolyphenyStatement) connection.createStatement() ) {
            try ( ResultSet result = result( statement, generatedKeys ) ) {
                assertFalse( result.isClosed() );
                when( client.isClosed() ).thenReturn( true );

                assertTrue( result.isClosed() );
                assertThrows( SQLException.class, result::next );
                verify( client, never() ).abort();
            }
        }
    }


    private static ResultSet result( PolyphenyStatement statement, boolean generatedKeys ) throws SQLException {
        ColumnMeta id = ColumnMeta.newBuilder().setColumnName( "id" ).setColumnLabel( "id" )
                .setTypeMeta( TypeMeta.newBuilder().setProtoValueType( ProtoPolyType.BIGINT ) ).build();
        Frame frame = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.newBuilder().addColumnMeta( id )
                .addRows( Row.newBuilder().addValues( TypedValue.fromLong( 1 ).serialize() ) ) ).build();
        if ( generatedKeys ) {
            statement.captureGeneratedKeys( StatementResult.newBuilder().setGeneratedKeys( frame ).build(), -1 );
            return statement.getGeneratedKeys();
        }
        return statement.publishResult( frame, -1 );
    }

}

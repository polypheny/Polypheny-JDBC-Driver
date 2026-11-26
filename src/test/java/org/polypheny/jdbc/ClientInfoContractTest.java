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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
import org.polypheny.prism.ConnectionResponse;

class ClientInfoContractTest {

    private static final String REMOVE = "PolyphenyRemoveClientInfoProperty";
    private static final String PARTIAL_BATCH = "PolyphenyPartialBatchResults";
    private static final String BOUNDARIES = "transactionBoundaryEvents";


    @ParameterizedTest
    @ValueSource(strings = { REMOVE, PARTIAL_BATCH, BOUNDARIES })
    void publicSettersRejectPrivateControlNamesBeforeSending( String name ) throws SQLException {
        RpcService rpc = mock( RpcService.class );
        try ( Connection connection = connection( rpc ) ) {
            SQLClientInfoException single = assertThrows( SQLClientInfoException.class, () -> connection.setClientInfo( name, "false" ) );
            assertEquals( ClientInfoStatus.REASON_UNKNOWN_PROPERTY, single.getFailedProperties().get( name ) );
            assertThrows( SQLClientInfoException.class, () -> connection.setClientInfo( name, null ) );

            Properties properties = new Properties();
            properties.setProperty( "ApplicationName", "Importer" );
            properties.setProperty( name, "ApplicationName" );
            SQLClientInfoException bulk = assertThrows( SQLClientInfoException.class, () -> connection.setClientInfo( properties ) );
            assertEquals( ClientInfoStatus.REASON_UNKNOWN_PROPERTY, bulk.getFailedProperties().get( name ) );
            verifyNoInteractions( rpc );
        }
    }


    @Test
    void readbackHidesPrivateControlsButPreservesCypherFormat() throws SQLException {
        RpcService rpc = mock( RpcService.class );
        when( rpc.getClientInfoProperties( any( ClientInfoPropertiesRequest.class ), anyInt() ) ).thenReturn( ClientInfoProperties.newBuilder()
                .putProperties( REMOVE, "" ).putProperties( PARTIAL_BATCH, "true" ).putProperties( BOUNDARIES, "true" )
                .putProperties( "CypherResultFormat", "relational" ).build() );
        try ( Connection connection = connection( rpc ) ) {
            Properties properties = connection.getClientInfo();
            assertEquals( "relational", properties.getProperty( "CypherResultFormat" ) );
            assertFalse( properties.containsKey( REMOVE ) );
            assertFalse( properties.containsKey( PARTIAL_BATCH ) );
            assertFalse( properties.containsKey( BOUNDARIES ) );
        }
    }


    @Test
    void bulkReplacementUsesOneReservationAndOneDeadline() throws Exception {
        RpcService rpc = mock( RpcService.class );
        runUnderReservation( rpc );
        when( rpc.getClientInfoProperties( any( ClientInfoPropertiesRequest.class ), any( RequestDeadline.class ) ) ).thenReturn( ClientInfoProperties.newBuilder()
                .putProperties( REMOVE, "" ).putProperties( PARTIAL_BATCH, "true" ).putProperties( BOUNDARIES, "true" )
                .putProperties( "ApplicationName", "Importer" ).putProperties( "ClientUser", "Operator" ).build() );
        try ( Connection connection = connection( rpc ) ) {
            Properties properties = new Properties();
            properties.setProperty( "CypherResultFormat", "relational" );
            connection.setClientInfo( properties );

            ArgumentCaptor<RequestDeadline> deadline = ArgumentCaptor.forClass( RequestDeadline.class );
            verify( rpc ).getClientInfoProperties( eq( ClientInfoPropertiesRequest.getDefaultInstance() ), deadline.capture() );
            ArgumentCaptor<ClientInfoProperties> changes = ArgumentCaptor.forClass( ClientInfoProperties.class );
            verify( rpc, times( 3 ) ).setClientInfoProperties( changes.capture(), same( deadline.getValue() ) );
            verify( rpc ).withWriteReservation( anyInt(), any() );
            List<ClientInfoProperties> requests = changes.getAllValues();
            assertEquals( ClientInfoProperties.newBuilder().putProperties( "CypherResultFormat", "relational" ).build(), requests.get( 0 ) );
            Set<String> removed = requests.subList( 1, 3 ).stream().map( request -> request.getPropertiesOrThrow( REMOVE ) ).collect( Collectors.toSet() );
            assertEquals( new HashSet<>( Arrays.asList( "ApplicationName", "ClientUser" ) ), removed );
        }
    }


    @Test
    void legacyBulkUpdateDoesNotSendRemovalCommands() throws Exception {
        RpcService rpc = mock( RpcService.class );
        runUnderReservation( rpc );
        when( rpc.getClientInfoProperties( any( ClientInfoPropertiesRequest.class ), any( RequestDeadline.class ) ) ).thenReturn(
                ClientInfoProperties.newBuilder().putProperties( "ApplicationName", "Importer" ).build() );
        try ( Connection connection = connection( rpc ) ) {
            Properties properties = new Properties();
            properties.setProperty( "ClientUser", "Operator" );
            connection.setClientInfo( properties );
            ArgumentCaptor<ClientInfoProperties> update = ArgumentCaptor.forClass( ClientInfoProperties.class );
            verify( rpc ).setClientInfoProperties( update.capture(), any( RequestDeadline.class ) );
            assertEquals( ClientInfoProperties.newBuilder().putProperties( "ClientUser", "Operator" ).build(), update.getValue() );
        }
    }


    @Test
    void negotiationStillEnablesPrivateControls() throws SQLException {
        RpcService rpc = mock( RpcService.class );
        when( rpc.connect( any(), eq( 1000 ) ) ).thenReturn( ConnectionResponse.newBuilder().setIsCompatible( true )
                .setMajorApiVersion( 2 ).setMinorApiVersion( 5 ).setSupportsGeneratedKeys( true ).build() );
        ConnectionString target = target();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );

        client.register( new PolyphenyConnectionProperties( target, client ), 1000 );

        verify( rpc ).setClientInfoProperties( ClientInfoProperties.newBuilder().putProperties( PARTIAL_BATCH, "true" )
                .putProperties( BOUNDARIES, "true" ).build(), 1000 );
        verify( rpc ).enableTransactionBoundaryEvents();
    }


    private static void runUnderReservation( RpcService rpc ) throws InterruptedException {
        when( rpc.withWriteReservation( anyInt(), any() ) ).thenAnswer( invocation -> {
            Transport.WriteOperation<?> operation = invocation.getArgument( 1 );
            operation.run();
            return true;
        } );
    }


    private static ConnectionString target() throws SQLException {
        return new ConnectionString( "jdbc:polypheny://localhost:20590" );
    }


    private static Connection connection( RpcService rpc ) throws SQLException {
        ConnectionString target = target();
        PrismInterfaceClient client = new PrismInterfaceClient( rpc );
        return new PolyConnection( new PolyphenyConnectionProperties( target, client ), new PolyphenyDatabaseMetadata( client, target ) );
    }

}

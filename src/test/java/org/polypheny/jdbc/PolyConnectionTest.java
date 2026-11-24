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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;

public class PolyConnectionTest {

    private PolyConnection connection;

    private PolyphenyConnectionProperties properties;

    @BeforeEach
    void setUp() {
        properties = mock( PolyphenyConnectionProperties.class );
        when( properties.getPrismInterfaceClient() ).thenReturn( mock( PrismInterfaceClient.class ) );
        connection = new PolyConnection( properties, mock( PolyphenyDatabaseMetadata.class ) );
    }


    @Test
    public void getAutoCommitWhenConnectionIsNotClosed() {
        assertDoesNotThrow( () -> connection.getAutoCommit() );

        verify( properties, times( 1 ) ).isAutoCommit();
    }


    @Test
    public void getAutoCommitWhenConnectionIsClosedThenThrowException() throws SQLException {
        connection.close();

        assertThrows( SQLException.class, () -> connection.getAutoCommit() );
    }


    @Test
    public void setAutoCommitWhenConnectionIsClosedThenThrowException() throws SQLException {
        connection.close();

        assertThrows( SQLException.class, () -> connection.setAutoCommit( true ) );
    }


    @Test
    public void setAutoCommitToTrue() throws SQLException {
        when( properties.isAutoCommit() ).thenReturn( false );
        when( properties.getNetworkTimeout() ).thenReturn( 5000 );
        PrismInterfaceClient client = properties.getPrismInterfaceClient();

        assertDoesNotThrow( () -> connection.setAutoCommit( true ) );

        InOrder requests = inOrder( client, properties );
        requests.verify( client ).commitTransaction( 5000 );
        requests.verify( properties ).setAutoCommit( true );
    }


    @Test
    public void setAutoCommitToFalse() throws PrismInterfaceServiceException {
        when( properties.isAutoCommit() ).thenReturn( true );
        when( properties.getNetworkTimeout() ).thenReturn( 5000 );

        assertDoesNotThrow( () -> connection.setAutoCommit( false ) );

        verify( properties, times( 1 ) ).setAutoCommit( false );
        verify( properties.getPrismInterfaceClient(), never() ).commitTransaction( 5000 );
    }


    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void settingTheCurrentAutoCommitModeIsANoOp( boolean autoCommit ) throws SQLException {
        when( properties.isAutoCommit() ).thenReturn( autoCommit );
        when( properties.getNetworkTimeout() ).thenReturn( 5000 );

        connection.setAutoCommit( autoCommit );

        verify( properties, never() ).setAutoCommit( autoCommit );
        verify( properties.getPrismInterfaceClient(), never() ).commitTransaction( 5000 );
    }


    @Test
    void enablingAutoCommitKeepsManualModeWhenCommitFails() throws SQLException {
        when( properties.isAutoCommit() ).thenReturn( false );
        when( properties.getNetworkTimeout() ).thenReturn( 5000 );
        PrismInterfaceClient client = properties.getPrismInterfaceClient();
        PrismInterfaceServiceException failure = new PrismInterfaceServiceException.ServerError( "Commit failed", "40001", 47 );
        doThrow( failure ).when( client ).commitTransaction( 5000 );

        assertSame( failure, assertThrows( SQLException.class, () -> connection.setAutoCommit( true ) ) );

        verify( properties, never() ).setAutoCommit( true );
    }

}

/*
 * Copyright 2019-2023 The Polypheny Project
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

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import org.junit.Test;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;

public class PolyphenyConnectionTest {

    private PolyphenyConnection connection;

    private PolyphenyConnectionProperties properties;

    private PolyphenyDatabaseMetadata databaseMetaData;


    @Test
    public void getAutoCommitWhenConnectionIsNotClosed() {
        properties = mock( PolyphenyConnectionProperties.class );
        databaseMetaData = mock( PolyphenyDatabaseMetadata.class );
        connection = new PolyphenyConnection( properties, databaseMetaData );

        try {
            connection.getAutoCommit();
        } catch ( SQLException e ) {
            fail( "Should not throw an exception" );
        }

        verify( properties, times( 1 ) ).isAutoCommit();
    }


    @Test
    public void getAutoCommitWhenConnectionIsClosedThenThrowException() throws SQLException {
        properties = mock( PolyphenyConnectionProperties.class );
        ProtoInterfaceClient protoInterfaceClient = mock( ProtoInterfaceClient.class );
        when( properties.getProtoInterfaceClient() ).thenReturn( protoInterfaceClient );
        databaseMetaData = mock( PolyphenyDatabaseMetadata.class );
        connection = new PolyphenyConnection( properties, databaseMetaData );
        connection.close();

        assertThrows( SQLException.class, () -> connection.getAutoCommit() );
    }


    @Test
    public void setAutoCommitWhenConnectionIsClosedThenThrowException() throws SQLException {
        properties = mock( PolyphenyConnectionProperties.class );
        ProtoInterfaceClient protoInterfaceClient = mock( ProtoInterfaceClient.class );
        when( properties.getProtoInterfaceClient() ).thenReturn( protoInterfaceClient );
        databaseMetaData = mock( PolyphenyDatabaseMetadata.class );
        connection = new PolyphenyConnection( properties, databaseMetaData );
        connection.close();

        assertThrows( SQLException.class, () -> connection.setAutoCommit( true ) );
    }


    @Test
    public void setAutoCommitToTrue() throws SQLException {
        properties = mock( PolyphenyConnectionProperties.class );
        databaseMetaData = mock( PolyphenyDatabaseMetadata.class );
        connection = new PolyphenyConnection( properties, databaseMetaData );
        connection.setAutoCommit( false );

        when( properties.isAutoCommit() ).thenReturn( false );

        try {
            connection.setAutoCommit( true );
        } catch ( SQLException e ) {
            fail( "Unexpected SQLException occurred: " + e.getMessage() );
        }

        verify( properties, times( 1 ) ).setAutoCommit( true );
    }


    @Test
    public void setAutoCommitToFalse() throws ProtoInterfaceServiceException {
        properties = mock( PolyphenyConnectionProperties.class );
        databaseMetaData = mock( PolyphenyDatabaseMetadata.class );
        connection = new PolyphenyConnection( properties, databaseMetaData );

        when( properties.isAutoCommit() ).thenReturn( true );
        when( properties.getNetworkTimeout() ).thenReturn( 5000 );

        try {
            connection.setAutoCommit( false );
        } catch ( SQLException e ) {
            fail( "Unexpected SQLException" );
        }

        verify( properties, times( 1 ) ).setAutoCommit( false );
    }

}
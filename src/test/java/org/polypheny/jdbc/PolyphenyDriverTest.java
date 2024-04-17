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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.jdbc.properties.PropertyUtils;

@ExtendWith(TestHelper.class)
public class PolyphenyDriverTest {

    private static final PolyphenyDriver DRIVER = new PolyphenyDriver();


    @Test()
    public void getParentLoggerThrowsException() {
        assertThrows( SQLFeatureNotSupportedException.class, DRIVER::getParentLogger );
    }


    @Test
    public void jdbcCompliantWhenDriverIsJdbcCompliant() {
        boolean jdbcCompliant = DRIVER.jdbcCompliant();
        assertEquals( DriverProperties.isJDBC_COMPLIANT(), jdbcCompliant );
    }


    @Test
    public void getMinorVersionReturnsCorrectVersion() {
        int expectedMinorVersion = DriverProperties.getDRIVER_MINOR_VERSION();
        int actualMinorVersion = DRIVER.getMinorVersion();

        assertEquals( expectedMinorVersion, actualMinorVersion );
    }


    @Test
    public void getMajorVersionReturnsCorrectVersion() {
        int expectedMajorVersion = DriverProperties.getDRIVER_MAJOR_VERSION();
        int actualMajorVersion = DRIVER.getMajorVersion();

        assertEquals( expectedMajorVersion, actualMajorVersion );
    }


    @Test
    public void getPropertyInfoWithValidUrlAndProperties() {
        String url = "jdbc:polypheny://testuser:testpassword@localhost:20591/database";

        try {
            DriverPropertyInfo[] propertyInfo = DRIVER.getPropertyInfo( url, null );

            assertEquals( 7, propertyInfo.length );

            assertEquals( "user", propertyInfo[0].name );
            assertEquals( "testuser", propertyInfo[0].value );
            assertEquals( "Specifies the username for authentication. If not specified, the database uses the default user.", propertyInfo[0].description );
            assertEquals( false, propertyInfo[0].required );

            assertEquals( "password", propertyInfo[1].name );
            assertEquals( "testpassword", propertyInfo[1].value );
            assertEquals( "Specifies the password associated with the given username. If not specified the database assumes that the user does not have a password.", propertyInfo[1].description );
            assertEquals( false, propertyInfo[1].required );

            assertEquals( "autocommit", propertyInfo[2].name );
            assertEquals( "true", propertyInfo[2].value );
            assertEquals( "Determines if each SQL statement is treated as a transaction.", propertyInfo[2].description );
            assertArrayEquals( new String[]{ "true", "false" }, propertyInfo[2].choices );

            assertEquals( "readonly", propertyInfo[3].name );
            assertEquals( "false", propertyInfo[3].value );
            assertEquals( "Indicates if the connection is in read-only mode. Currently ignored, reserved for future use.", propertyInfo[3].description );
            assertArrayEquals( new String[]{ "true", "false" }, propertyInfo[3].choices );

            assertEquals( "holdability", propertyInfo[4].name );
            assertEquals( "CLOSE", propertyInfo[4].value );
            assertEquals( "Specifies the holdability of ResultSet objects.", propertyInfo[4].description );
            assertArrayEquals( new String[]{ "HOLD", "CLOSE" }, propertyInfo[4].choices );

            assertEquals( "isolation", propertyInfo[5].name );
            assertEquals( "COMMITTED", propertyInfo[5].value );
            assertEquals( "Indicates the transaction isolation level.", propertyInfo[5].description );
            assertArrayEquals( new String[]{ "COMMITTED", "DIRTY", "SERIALIZABLE", "REPEATABLE_READ" }, propertyInfo[5].choices );

            assertEquals( "nwtimeout", propertyInfo[6].name );
            assertEquals( "0", propertyInfo[6].value );
            assertEquals( "Specifies the network timeout in seconds. Corresponds to the JDBC network timeout.", propertyInfo[6].description );

        } catch ( SQLException e ) {
            fail( "An exception occurred: " + e.getMessage() );
        }
    }


    @Test
    public void getPropertyInfoWithDefaultValuesWhenPropertiesNotProvided() {
        String url = "jdbc:polypheny://localhost:20591/database";
        Properties properties = new Properties();

        try {
            DriverPropertyInfo[] infoProperties = DRIVER.getPropertyInfo( url, properties );

            assertEquals( 7, infoProperties.length );

            assertEquals( PropertyUtils.getUSERNAME_KEY(), infoProperties[0].name );
            assertEquals( null, infoProperties[0].value );
            assertEquals( "Specifies the username for authentication. If not specified, the database uses the default user.", infoProperties[0].description );
            assertEquals( false, infoProperties[0].required );

            assertEquals( PropertyUtils.getPASSWORD_KEY(), infoProperties[1].name );
            assertEquals( null, infoProperties[1].value );
            assertEquals( "Specifies the password associated with the given username. If not specified the database assumes that the user does not have a password.", infoProperties[1].description );
            assertEquals( false, infoProperties[1].required );

            assertEquals( PropertyUtils.getAUTOCOMMIT_KEY(), infoProperties[2].name );
            assertEquals( String.valueOf( PropertyUtils.isDEFAULT_AUTOCOMMIT() ), infoProperties[2].value );
            assertEquals( "Determines if each SQL statement is treated as a transaction.", infoProperties[2].description );
            assertArrayEquals( new String[]{ "true", "false" }, infoProperties[2].choices );

            assertEquals( PropertyUtils.getREAD_ONLY_KEY(), infoProperties[3].name );
            assertEquals( String.valueOf( PropertyUtils.isDEFAULT_READ_ONLY() ), infoProperties[3].value );
            assertEquals( "Indicates if the connection is in read-only mode. Currently ignored, reserved for future use.", infoProperties[3].description );
            assertArrayEquals( new String[]{ "true", "false" }, infoProperties[3].choices );

            assertEquals( PropertyUtils.getRESULT_SET_HOLDABILITY_KEY(), infoProperties[4].name );
            assertEquals( PropertyUtils.getHoldabilityName( PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY() ), infoProperties[4].value );
            assertEquals( "Specifies the holdability of ResultSet objects.", infoProperties[4].description );
            assertArrayEquals( new String[]{ "HOLD", "CLOSE" }, infoProperties[4].choices );

            assertEquals( PropertyUtils.getTRANSACTION_ISOLATION_KEY(), infoProperties[5].name );
            assertEquals( PropertyUtils.getTransactionIsolationName( PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION() ), infoProperties[5].value );
            assertEquals( "Indicates the transaction isolation level.", infoProperties[5].description );
            assertArrayEquals( new String[]{ "COMMITTED", "DIRTY", "SERIALIZABLE", "REPEATABLE_READ" }, infoProperties[5].choices );

            assertEquals( PropertyUtils.getNETWORK_TIMEOUT_KEY(), infoProperties[6].name );
            assertEquals( String.valueOf( PropertyUtils.getDEFAULT_NETWORK_TIMEOUT() ), infoProperties[6].value );
            assertEquals( "Specifies the network timeout in seconds. Corresponds to the JDBC network timeout.", infoProperties[6].description );

        } catch ( SQLException e ) {
            fail( "An exception occurred: " + e.getMessage() );
        }
    }


    @Test
    public void getPropertyInfoWithUserSpecifiedValuesWhenPropertiesProvided() {
        String url = "jdbc:polypheny://localhost:20591/database";
        Properties properties = new Properties();
        properties.setProperty( "user", "testuser" );
        properties.setProperty( "password", "testpassword" );
        properties.setProperty( "autocommit", "false" );
        properties.setProperty( "readonly", "true" );
        properties.setProperty( "holdability", "HOLD" );
        properties.setProperty( "isolation", "DIRTY" );
        properties.setProperty( "nwtimeout", "10" );

        try {
            DriverPropertyInfo[] propertyInfo = DRIVER.getPropertyInfo( url, properties );

            assertEquals( 7, propertyInfo.length );

            assertEquals( "user", propertyInfo[0].name );
            assertEquals( "testuser", propertyInfo[0].value );
            assertEquals( "Specifies the username for authentication. If not specified, the database uses the default user.", propertyInfo[0].description );
            assertEquals( false, propertyInfo[0].required );

            assertEquals( "password", propertyInfo[1].name );
            assertEquals( "testpassword", propertyInfo[1].value );
            assertEquals( "Specifies the password associated with the given username. If not specified the database assumes that the user does not have a password.", propertyInfo[1].description );
            assertEquals( false, propertyInfo[1].required );

            assertEquals( "autocommit", propertyInfo[2].name );
            assertEquals( "false", propertyInfo[2].value );
            assertEquals( "Determines if each SQL statement is treated as a transaction.", propertyInfo[2].description );
            assertArrayEquals( new String[]{ "true", "false" }, propertyInfo[2].choices );

            assertEquals( "readonly", propertyInfo[3].name );
            assertEquals( "true", propertyInfo[3].value );
            assertEquals( "Indicates if the connection is in read-only mode. Currently ignored, reserved for future use.", propertyInfo[3].description );
            assertArrayEquals( new String[]{ "true", "false" }, propertyInfo[3].choices );

            assertEquals( "holdability", propertyInfo[4].name );
            assertEquals( "HOLD", propertyInfo[4].value );
            assertEquals( "Specifies the holdability of ResultSet objects.", propertyInfo[4].description );
            assertArrayEquals( new String[]{ "HOLD", "CLOSE" }, propertyInfo[4].choices );

            assertEquals( "isolation", propertyInfo[5].name );
            assertEquals( "DIRTY", propertyInfo[5].value );
            assertEquals( "Indicates the transaction isolation level.", propertyInfo[5].description );
            assertArrayEquals( new String[]{ "COMMITTED", "DIRTY", "SERIALIZABLE", "REPEATABLE_READ" }, propertyInfo[5].choices );

            assertEquals( "nwtimeout", propertyInfo[6].name );
            assertEquals( "10", propertyInfo[6].value );
            assertEquals( "Specifies the network timeout in seconds. Corresponds to the JDBC network timeout.", propertyInfo[6].description );

        } catch ( SQLException e ) {
            fail( "An exception occurred: " + e.getMessage() );
        }
    }


    @Test
    public void acceptsURL_String__CorrectDriverSchema() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( DriverProperties.getDRIVER_URL_SCHEMA() );

        assertEquals( expected, actual );
    }


    @Test()
    public void acceptsURL_null() {
        assertThrows( SQLException.class, () -> DRIVER.acceptsURL( null ) );
    }


    @Test
    public void acceptsURL_EmptyString() throws Exception {
        final boolean expected = false;
        final boolean actual = DRIVER.acceptsURL( "" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__NoJdbcSchema() throws Exception {
        final boolean expected = false;
        final boolean actual = DRIVER.acceptsURL( "polypheny://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__NoPolyphenySubSchema() throws Exception {
        final boolean expected = false;
        final boolean actual = DRIVER.acceptsURL( "jdbc://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__WrongSubSchema() throws Exception {
        final boolean expected = false;
        final boolean actual = DRIVER.acceptsURL( "jdbc:foo://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrl() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__MissingCredentials() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://host:20569" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrl_NewUrlStyle() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny:http://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrl_NewUrlStyleHttps() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny:https://username:password@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoPassword() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username@host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoParameters() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host:20569/database" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoDatabase() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host:20569?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoPort() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoHost() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlNoUsernamePassword() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://host:20569/database?k1=v1&k2=v2" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__AcceptableUrlDefaultsOnly() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny:///" );

        assertEquals( expected, actual );
    }


    @Test
    public void acceptsURL_String__MalformedParameter() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host:20569/database?k1=v1&k2" ); // k2 is ignored!

        assertEquals( expected, actual );
    }

}

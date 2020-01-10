/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2020 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.polypheny.jdbc;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 *
 */
public class DriverTest {


    private static final Driver DRIVER = new Driver();


    @BeforeClass
    public static void setUpClass() {
    }


    @AfterClass
    public static void tearDownClass() {
    }


    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {
    }


    @Test(expected = SQLException.class)
    public void acceptsURL_null() throws Exception {
        final boolean actual = DRIVER.acceptsURL( null );
        fail( "No SQLException thrown" );
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
    public void acceptsURL_String__MalformedParameter() throws Exception {
        final boolean expected = true;
        final boolean actual = DRIVER.acceptsURL( "jdbc:polypheny://username:password@host:20569/database?k1=v1&k2" ); // k2 is ignored!

        assertEquals( expected, actual );
    }


    @Test
    public void parseUrl_String_null__AcceptableUrl() throws Exception {
        final Properties expected = new Properties();
        expected.setProperty( Driver.PROPERTY_USERNAME_KEY, "username" );
        expected.setProperty( Driver.PROPERTY_PASSWORD_KEY, "password" );
        expected.setProperty( Driver.PROPERTY_HOST_KEY, "localhost" );
        expected.setProperty( Driver.PROPERTY_PORT_KEY, "20569" );
        expected.setProperty( Driver.PROPERTY_DATABASE_KEY, "database" );
        expected.setProperty( "k1", "v1" );
        expected.setProperty( "k2", "v2" );
        expected.setProperty( Driver.PROPERTY_URL_KEY, "http://localhost:20569/" );

        final Properties actual = DRIVER.parseUrl( "jdbc:polypheny://username:password@localhost:20569/database?k1=v1&k2=v2", null );

        assertEquals( expected, actual );
    }


    @Test
    public void parseUrl_String_null__AcceptableUrlNoHost() throws Exception {
        final String expected = "localhost";
        final String actual = DRIVER
                .parseUrl( "jdbc:polypheny://username:password@:20569/database?k1=v1&k2=v2", null )
                .getProperty( Driver.PROPERTY_HOST_KEY );

        assertEquals( expected, actual );
    }


    @Test
    public void parseUrl_String__AcceptableUrlNoPort() throws Exception {
        final int expected = Driver.DEFAULT_PORT;
        final Properties connectionProperties = DRIVER.parseUrl( "jdbc:polypheny://username:password@host/database?k1=v1&k2=v2", new Properties() );
        final int actual = Integer.parseInt( connectionProperties.getProperty( "port" ) );

        assertEquals( expected, actual );
    }


    @Test
    public void parseUrl_String_Properties__AcceptableUrl_OverrideHost() throws Exception {
        final String expected = "someother-host";
        final Properties info = new Properties();
        info.setProperty( Driver.PROPERTY_HOST_KEY, expected );

        final String actual = DRIVER
                .parseUrl( "jdbc:polypheny://username:password@localhost:20569/database?k1=v1&k2=v2", info )
                .getProperty( Driver.PROPERTY_HOST_KEY );

        assertEquals( expected, actual );
    }


    @Test
    public void parseUrl_String_Properties__AcceptableUrl_SetViaUrlParam() throws Exception {
        final Properties expected = new Properties();
        expected.setProperty( Driver.PROPERTY_USERNAME_KEY, "username" );
        expected.setProperty( Driver.PROPERTY_PASSWORD_KEY, "secret" );
        expected.setProperty( Driver.PROPERTY_HOST_KEY, "localhost" );
        expected.setProperty( Driver.PROPERTY_PORT_KEY, "20569" );
        expected.setProperty( Driver.PROPERTY_DATABASE_KEY, "database" );
        expected.setProperty( "k1", "v1" );
        expected.setProperty( "k2", "v2" );
        expected.setProperty( Driver.PROPERTY_URL_KEY, "http://localhost:20569/" );

        final Properties actual = DRIVER.parseUrl( "jdbc:polypheny://username@localhost:20569/database?k1=v1&k2=v2&" + Driver.PROPERTY_PASSWORD_KEY + "=secret", null );

        assertEquals( expected, actual );
    }


    @Test(expected = SQLException.class)
    public void connect_null_null() throws Exception {
        DRIVER.connect( null, null );
        fail( "No SQLException thrown" );
    }


    @Test
    public void connect_EmptyString_null() throws Exception {
        final java.sql.Connection expected = null;
        final java.sql.Connection actual = DRIVER.connect( "", null );

        assertEquals( expected, actual );
    }


    @Test
    public void connect_String_null__WrongSchema() throws Exception {
        final java.sql.Connection expected = null;
        final java.sql.Connection actual = DRIVER.connect( "foo:polypheny://username:password@localhost:20569/database?k1=v1&k2=v2", null );

        assertEquals( expected, actual );
    }


    @Test
    public void connect_String_null__WrongSubSchema() throws Exception {
        final java.sql.Connection expected = null;
        final java.sql.Connection actual = DRIVER.connect( "jdbc:foo://username:password@localhost:20569/database?k1=v1&k2=v2", null );

        assertEquals( expected, actual );
    }
}

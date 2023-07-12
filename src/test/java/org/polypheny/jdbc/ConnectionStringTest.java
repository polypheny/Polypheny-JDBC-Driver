package org.polypheny.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.jdbc.properties.PropertyUtils;

public class ConnectionStringTest {

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
    public void connectionString_String__null() throws Exception {
        final String url = null;
        final ConnectionString cs = new ConnectionString( url );
        fail( "No SQLException thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__Empty() throws Exception {
        final String url = "";
        final ConnectionString cs = new ConnectionString( url );
        fail( "No SQLException thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__NoJdbcSchema() throws Exception {
        final String url = "polypheny://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );
        fail( "No SQLException thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__NoPolyphenySubSchema() throws Exception {
        final String url = "jdbc://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );
        fail( "No SQLException thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__WrongSubSchema() throws Exception {
        final String url = "jdbc:foo://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );
        fail( "No SQLException thrown" );
    }


    @Test
    public void connectionString_String__MissingCredentials() throws Exception {
        final String expectedTarget = "host:20590";

        final String url = "jdbc:polypheny://host:20590";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__AcceptableUrl() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "password" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "namespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__ColumnInPassword() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "pass:word" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "namespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny://username:pass:word@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__MissingValue() throws Exception {
        final String url = "jdbc:polypheny://username:pass:word@localhost:20569/namespace?k1=v1&k2";
        final ConnectionString cs = new ConnectionString( url );

        fail( "No sql exception thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__MissplacedAt() throws Exception {
        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1@v1&k2";
        final ConnectionString cs = new ConnectionString( url );

        fail( "No sql exception thrown" );
    }


    @Test(expected = SQLException.class)
    public void connectionString_String__MissplacedAt2() throws Exception {
        final String url = "jdbc:polypheny://username@password:localhost:20569/namespace?k1@v1&k2";
        final ConnectionString cs = new ConnectionString( url );

        fail( "No sql exception thrown" );
    }


    @Test
    public void connectionString_String__AcceptableNewStyleUrlHttp() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "password" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "namespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny:http://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__AcceptableNewStyleUrlHttps() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "password" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "namespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny:https://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__SchemaOnly() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        final String target = PropertyUtils.getDEFAULT_HOST() + ":" + PropertyUtils.getDEFAULT_PORT();

        final String url = "jdbc:polypheny://";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( target, cs.getTarget() );
    }


    @Test
    public void connectionString_String__NoPort() throws Exception {
        final String expectedTarget = "host:" + PropertyUtils.getDEFAULT_PORT();
        final String url = "jdbc:polypheny://username:password@host/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__NoHost() throws Exception {
        final String expectedTarget = PropertyUtils.getDEFAULT_HOST() + ":20569";
        final String url = "jdbc:polypheny://username:password@:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String_Parameters__null() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "password" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "namespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );

        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url, null );

        assertEquals( expected, cs.getParameters() );
    }


    @Test
    public void connectionString_String_Parameters__ImportAndOverwrite() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "urlUsername" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "urlPassword" );
        expected.put( PropertyUtils.getNAMESPACE_KEY(), "mapNamespace" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        expected.put( "k3", "v3" );
        expected.put( "k4", "v4" );

        final Properties properties = new Properties();
        properties.setProperty( PropertyUtils.getUSERNAME_KEY(), "mapUsername" );
        properties.setProperty( PropertyUtils.getPASSWORD_KEY(), "mapPassword" );
        properties.setProperty( PropertyUtils.getNAMESPACE_KEY(), "mapNamespace" );
        properties.setProperty( "k1", "v1" );
        properties.setProperty( "k2", "v2" );

        final String url = "jdbc:polypheny://urlUsername:urlPassword@localhost:20569/?k3=v3&k4=v4";
        final ConnectionString cs = new ConnectionString( url, properties );

        assertEquals( expected, cs.getParameters() );
    }


    @Test
    public void connectionString_String__Ipv6() throws SQLException {
        final String expectedTarget = "[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]:12345";

        final String url = "jdbc:polypheny://username:password@[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]:12345/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
    }


}

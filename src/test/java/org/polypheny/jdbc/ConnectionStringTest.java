package org.polypheny.jdbc;

import org.junit.*;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        final ConnectionString cs = new ConnectionString(url);
        fail("No SQLException thrown");
    }

    @Test(expected = SQLException.class)
    public void connectionString_String__Empty() throws Exception {
        final String url = "";
        final ConnectionString cs = new ConnectionString(url);
        fail("No SQLException thrown");
    }

    @Test(expected = SQLException.class)
    public void connectionString_String__NoJdbcSchema() throws Exception {
        final String url = "polypheny://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);
        fail("No SQLException thrown");
    }

    @Test(expected = SQLException.class)
    public void connectionString_String__NoPolyphenySubSchema() throws Exception {
        final String url = "jdbc://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);
        fail("No SQLException thrown");
    }

    @Test(expected = SQLException.class)
    public void connectionString_String__WrongSubSchema() throws Exception {
        final String url = "jdbc:foo://username:password@host:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);
        fail("No SQLException thrown");
    }

    @Test
    public void connectionString_String__AcceptableUrl() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, "username");
        expected.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "password");
        expected.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "namespace");
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expected, cs.getParameters());
        assertEquals(expectedTarget, cs.getTarget());
    }

    @Test
    public void connectionString_String__AcceptableNewStyleUrlHttp() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, "username");
        expected.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "password");
        expected.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "namespace");
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny:http://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expected, cs.getParameters());
        assertEquals(expectedTarget, cs.getTarget());
    }

    @Test
    public void connectionString_String__AcceptableNewStyleUrlHttps() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, "username");
        expected.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "password");
        expected.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "namespace");
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny:https://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expected, cs.getParameters());
        assertEquals(expectedTarget, cs.getTarget());
    }

    @Test
    public void connectionString_String__SchemaOnly() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        final String target = PolyphenyDriver.DEFAULT_HOST + ":" + PolyphenyDriver.DEFAULT_PORT;

        final String url = "jdbc:polypheny://";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expected, cs.getParameters());
        assertEquals(target, cs.getTarget());
    }

    @Test
    public void connectionString_String__NoPort() throws Exception {
        final String expectedTarget = "host:" + PolyphenyDriver.DEFAULT_PORT;
        final String url = "jdbc:polypheny://username:password@host/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expectedTarget, cs.getTarget());
    }

    @Test
    public void connectionString_String__NoHost() throws Exception {
        final String expectedTarget = PolyphenyDriver.DEFAULT_HOST + ":20569";
        final String url = "jdbc:polypheny://username:password@:20569/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expectedTarget, cs.getTarget());
    }

    @Test
    public void connectionString_String_Parameters__null() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, "username");
        expected.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "password");
        expected.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "namespace");
        expected.put("k1", "v1");
        expected.put("k2", "v2");

        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url, null);

        assertEquals(expected, cs.getParameters());
    }

    @Test
    public void connectionString_String_Parameters__ImportAndOverwrite() throws SQLException {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, "urlUsername");
        expected.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "urlPassword");
        expected.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "mapNamespace");
        expected.put("k1", "v1");
        expected.put("k2", "v2");
        expected.put("k3", "v3");
        expected.put("k4", "v4");

        final Properties properties = new Properties();
        properties.setProperty(PolyphenyDriver.PROPERTY_USERNAME_KEY, "mapUsername");
        properties.setProperty(PolyphenyDriver.PROPERTY_PASSWORD_KEY, "mapPassword");
        properties.setProperty(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, "mapNamespace");
        properties.setProperty("k1", "v1");
        properties.setProperty("k2", "v2");


        final String url = "jdbc:polypheny://urlUsername:urlPassword@localhost:20569/?k3=v3&k4=v4";
        final ConnectionString cs = new ConnectionString(url, properties);

        assertEquals(expected, cs.getParameters());
    }

    @Test
    public void connectionString_String__Ipv6() throws SQLException {
        final String expectedTarget = "[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]:12345";

        final String url = "jdbc:polypheny://username:password@[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]:12345/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString(url);

        assertEquals(expectedTarget, cs.getTarget());
    }


}

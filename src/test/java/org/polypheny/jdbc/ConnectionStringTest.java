package org.polypheny.jdbc;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.properties.PropertyUtils;

public class ConnectionStringTest {

    @Test()
    public void connectionString_String__null()  {
        assertThrows( SQLException.class, () -> new ConnectionString( null ) );
    }


    @Test()
    public void connectionString_String__Empty()  {
        assertThrows( SQLException.class, () -> new ConnectionString( "" ));
    }


    @Test()
    public void connectionString_String__NoJdbcSchema()  {
        final String url = "polypheny://username:password@host:20569/database?k1=v1&k2=v2";
        assertThrows( SQLException.class, () -> new ConnectionString( url ));
    }


    @Test()
    public void connectionString_String__NoPolyphenySubSchema()  {
        final String url = "jdbc://username:password@host:20569/database?k1=v1&k2=v2";
        assertThrows( SQLException.class, () ->new ConnectionString( url ));
    }


    @Test()
    public void connectionString_String__WrongSubSchema() throws Exception {
        final String url = "jdbc:foo://username:password@host:20569/database?k1=v1&k2=v2";
        assertThrows( SQLException.class, () -> new ConnectionString( url ));
    }


    @Test
    public void connectionString_String__MissingCredentials() throws Exception {
        final String expectedTarget = "host:20590";

        final String url = "jdbc:polypheny://host:20590";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__NoUsername()  {

    }


    @Test
    public void connecitonString_String__NoPassword() throws Exception {
        final String expectedUsername = "username";

        final String url = "jdbc:polypheny://username@localhost:20569";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedUsername, cs.getUser() );
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
    public void connectionString_String__AcceptableUrlNoNamespace() throws Exception {
        final HashMap<String, String> expected = new HashMap<>();
        expected.put( PropertyUtils.getUSERNAME_KEY(), "username" );
        expected.put( PropertyUtils.getPASSWORD_KEY(), "password" );
        expected.put( "k1", "v1" );
        expected.put( "k2", "v2" );
        final String expectedTarget = "localhost:20569";

        final String url = "jdbc:polypheny://username:password@localhost:20569?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expected, cs.getParameters() );
        assertEquals( expectedTarget, cs.getTarget() );
    }


    @Test
    public void connectionString_String__ColonInPassword() throws Exception {
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


    @Test()
    public void connectionString_String__MissingValue(){
        final String url = "jdbc:polypheny://username:pass:word@localhost:20569/namespace?k1=v1&k2";
        assertThrows( SQLException.class, () -> new ConnectionString( url ));
    }


    @Test()
    public void connectionString_String__MissplacedAt() {
        final String url = "jdbc:polypheny://username:password@localhost:20569/namespace?k1@v1&k2";
        assertThrows( SQLException.class, () -> new ConnectionString( url ));
    }


    @Test()
    public void connectionString_String__MissplacedAt2() throws Exception {
        final String url = "jdbc:polypheny://username@password:localhost:20569/namespace?k1@v1&k2";
        assertThrows( SQLException.class, () -> new ConnectionString( url ));
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
    @Disabled
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
        final String ip = "[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]";
        final int port = 12345;

        final String url = "jdbc:polypheny://username:password@[7ed0:1b1d:0058:6765:253f:2f64:6406:063c]:12345/database?k1=v1&k2=v2";
        final ConnectionString cs = new ConnectionString( url );

        assertEquals( expectedTarget, cs.getTarget() );
        assertEquals( ip, cs.getHost() );
        assertEquals( port, cs.getPort() );
    }

}

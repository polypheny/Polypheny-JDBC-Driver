package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

public class PolyphenyDriver implements java.sql.Driver {
    public static final String DRIVER_URL_SCHEMA = "jdbc:polypheny:";
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 20591;

    public static final String PROPERTY_USERNAME_KEY = "user";
    @java.lang.SuppressWarnings(
            "squid:S2068"
            // Credentials should not be hard-coded: 'password' detected
            // Justification: "password" is here the key to set the password in the connection parameters.
    )
    public static final String PROPERTY_PASSWORD_KEY = "password";
    public static final String PROPERTY_NAMESPACE_KEY = "namespace";

    /*
     * Connection urls ar of the form jdbc:polypheny://host:port/namespace?key1=value1&key2=value2
     */
    @Override
    public Connection connect(String url, Properties properties) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        ConnectionString connectionString = new ConnectionString(url, properties);
        ProtoInterfaceClient protoInterfaceClient = new ProtoInterfaceClient(connectionString.getTarget());
        protoInterfaceClient.connect(connectionString.getParameters());
        return new PolyphenyConnection(protoInterfaceClient);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL must no be null.");
        }
        return url.startsWith(DRIVER_URL_SCHEMA);
    }


    @Override
    public DriverPropertyInfo[] getPropertyInfo(String s, Properties properties) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
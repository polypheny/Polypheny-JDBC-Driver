package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.DriverProperties;

public class PolyphenyDriver implements java.sql.Driver {
    static {
        new PolyphenyDriver().register();
    }


    private void register() {
        try {
            DriverManager.registerDriver( this );
        } catch ( SQLException e ) {
            System.out.println(
                    "Error occurred while registering JDBC driver "
                            + this + ": " + e.toString() );
        }
    }


    @Override
    public Connection connect( String url, Properties properties ) throws SQLException {
        if ( !acceptsURL( url ) ) {
            return null;
        }
        ConnectionString connectionString = new ConnectionString( url, properties );
        ProtoInterfaceClient protoInterfaceClient = new ProtoInterfaceClient( connectionString.getTarget() );
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties(connectionString, protoInterfaceClient);
        PolyphenyDatabaseMetadata databaseMetadata = new PolyphenyDatabaseMetadata( protoInterfaceClient, connectionString );
        protoInterfaceClient.register( connectionProperties );
        return new PolyphenyConnection( connectionProperties, databaseMetadata );
    }


    @Override
    public boolean acceptsURL( String url ) throws SQLException {
        if ( url == null ) {
            throw new SQLException( "URL must no be null." );
        }
        return url.startsWith( DriverProperties.getDRIVER_URL_SCHEMA() );
    }


    @Override
    public DriverPropertyInfo[] getPropertyInfo( String s, Properties properties ) throws SQLException {
        return new DriverPropertyInfo[0];
    }


    @Override
    public int getMajorVersion() {
        return DriverProperties.getDRIVER_MAJOR_VERSION();
    }


    @Override
    public int getMinorVersion() {
        return DriverProperties.getDRIVER_MINOR_VERSION();
    }


    @Override
    public boolean jdbcCompliant() {
        return DriverProperties.isJDBC_COMPLIANT();
    }


    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}

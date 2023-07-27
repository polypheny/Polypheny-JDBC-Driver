package org.polypheny.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;
import org.polypheny.jdbc.meta.PolyphenyDatabaseMetadata;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.proto.ConnectionReply;

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
        PolyphenyConnectionProperties connectionProperties = new PolyphenyConnectionProperties( connectionString, protoInterfaceClient );
        PolyphenyDatabaseMetadata databaseMetadata = new PolyphenyDatabaseMetadata( protoInterfaceClient, connectionString );
        ConnectionReply connectionReply = protoInterfaceClient.register( connectionProperties, connectionProperties.getNetworkTimeout() );
        if ( connectionReply.hasHeartbeatInterval() ) {
            return new PolyphenyConnection( connectionProperties, databaseMetadata, connectionReply.getHeartbeatInterval() );
        }
        return new PolyphenyConnection( connectionProperties, databaseMetadata );
    }


    @Override
    public boolean acceptsURL( String url ) throws SQLException {
        if ( url == null ) {
            throw new ProtoInterfaceServiceException( SQLErrors.VALUE_ILLEGAL, "URL must no be null." );
        }
        return url.startsWith( DriverProperties.getDRIVER_URL_SCHEMA() );
    }


    @Override
    public DriverPropertyInfo[] getPropertyInfo( String url, Properties properties ) throws SQLException {
        ConnectionString connectionString = new ConnectionString( url, properties );

        DriverPropertyInfo[] infoProperties = new DriverPropertyInfo[7];

        // User Property
        infoProperties[0] = new DriverPropertyInfo(
                PropertyUtils.getUSERNAME_KEY(),
                connectionString.getUser() );
        infoProperties[0].description = "Specifies the username for authentication. If not specified, the database uses the default user.";
        infoProperties[0].required = false;

        // Password Property
        infoProperties[1] = new DriverPropertyInfo(
                PropertyUtils.getPASSWORD_KEY(),
                connectionString.getParameter( PropertyUtils.getPASSWORD_KEY() ));
        infoProperties[1].description = "Specifies the password associated with the given username. If not specified the database assumes that the user does not have a password.";
        infoProperties[1].required = false;

        // Autocommit Property
        infoProperties[2] = new DriverPropertyInfo(
                PropertyUtils.getAUTOCOMMIT_KEY(),
                connectionString.getParameter( PropertyUtils.getAUTOCOMMIT_KEY() ) );
        infoProperties[2].description = "Determines if each SQL statement is treated as a transaction.";
        infoProperties[2].choices = new String[]{ "true", "false" };

        // Readonly Property
        infoProperties[3] = new DriverPropertyInfo(
                PropertyUtils.getREAD_ONLY_KEY(),
                connectionString.getParameter( PropertyUtils.getREAD_ONLY_KEY() ) );
        infoProperties[3].description = "Indicates if the connection is in read-only mode. Currently ignored, reserved for future use.";
        infoProperties[3].choices = new String[]{ "true", "false" };

        // Holdability Property
        infoProperties[4] = new DriverPropertyInfo(
                PropertyUtils.getRESULT_SET_HOLDABILITY_KEY(),
                connectionString.getParameter(PropertyUtils.getRESULT_SET_HOLDABILITY_KEY()) );
        infoProperties[4].description = "Specifies the holdability of ResultSet objects.";
        infoProperties[4].choices = new String[]{ "HOLD", "CLOSE" };

        // Isolation Property
        infoProperties[5] = new DriverPropertyInfo(
                PropertyUtils.getTRANSACTION_ISOLATION_KEY(),
                connectionString.getParameter( PropertyUtils.getTRANSACTION_ISOLATION_KEY() ) );
        infoProperties[5].description = "Indicates the transaction isolation level.";
        infoProperties[5].choices = new String[]{ "COMMITTED", "DIRTY", "SERIALIZABLE", "REPEATABLE_READ" };

        // Network Timeout Property
        infoProperties[6] = new DriverPropertyInfo(
                PropertyUtils.getNETWORK_TIMEOUT_KEY(),
                connectionString.getParameter( PropertyUtils.getNETWORK_TIMEOUT_KEY() ));
        infoProperties[6].description = "Specifies the network timeout in seconds. Corresponds to the JDBC network timeout.";

        return infoProperties;
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
        throw new SQLFeatureNotSupportedException();
    }

}

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


import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;
import org.apache.calcite.avatica.remote.AvaticaHttpClient;
import org.apache.calcite.avatica.remote.AvaticaHttpClientFactory;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.RemoteProtobufService;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.OpenConnectionRequest;
import org.apache.calcite.avatica.remote.Service.OpenConnectionResponse;


@Slf4j
public class Driver extends UnregisteredDriver {

    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 20591;
    public static final String URL_SCHEMA = "jdbc:polypheny:";
    static final String PROPERTY_USERNAME_KEY = "user";
    @java.lang.SuppressWarnings(
            "squid:S2068"
            // Credentials should not be hard-coded: 'password' detected
            // Justification: "password" is here the key to set the password in the connection parameters.
    )
    static final String PROPERTY_PASSWORD_KEY = "password";
    static final String PROPERTY_URL_KEY = "url";
    static final String PROPERTY_HOST_KEY = "host";
    static final String PROPERTY_PORT_KEY = "port";
    static final String PROPERTY_DATABASE_KEY = "db";


    static {
        new Driver().register();
    }


    public Driver() {
        super();
    }


    @Override
    protected DriverVersion createDriverVersion() {
        return DriverVersion.load(
                Driver.class,
                "org-polypheny-jdbc.properties",
                "Polypheny JDBC Driver",
                "unknown version",
                "Polypheny",
                "unknown version" );
    }


    @Override
    protected String getConnectStringPrefix() {
        return URL_SCHEMA;
    }


    @Override
    public boolean acceptsURL( final String url ) throws SQLException {
        if ( url == null ) {
            throw new SQLException( new NullPointerException( "url == null" ) );
        }
        return url.toLowerCase().startsWith( getConnectStringPrefix().toLowerCase() );
    }


    @Override
    public Meta createMeta( AvaticaConnection connection ) {
        final ConnectionConfig config = connection.config();
        // Create a single Service and set it on the Connection instance
        final Service service = createService( connection, config );
        connection.setService( service );
        return new RemotePolyphenyMeta( connection );
    }


    protected Service createService( AvaticaConnection connection, ConnectionConfig config ) {
        final Service.Factory metaFactory = config.factory();
        final Service service;
        if ( metaFactory != null ) {
            service = metaFactory.create( connection );
        } else if ( config.url() != null ) {
            // TODO: let the user chose between JSON and Google Protocol Buffers
            //service = new RemoteService( getHttpClient( connection, config ) );
            service = new RemoteProtobufService( getHttpClient( connection, config ), new ProtobufTranslationImpl() );
        } else {
            throw new RuntimeException( new NullPointerException( "config.url() == null" ) );
            //service = new MockJsonService( Collections.<String, String>emptyMap() );
        }
        return service;
    }


    /**
     * Creates the HTTP client that communicates with the Avatica server.
     *
     * @param connection The {@link AvaticaConnection}.
     * @param config The configuration.
     * @return An {@link AvaticaHttpClient} implementation.
     */
    protected AvaticaHttpClient getHttpClient( AvaticaConnection connection, ConnectionConfig config ) {
        URL url;
        try {
            url = new URL( config.url() );
        } catch ( MalformedURLException e ) {
            throw new RuntimeException( e );
        }

        AvaticaHttpClientFactory httpClientFactory = config.httpClientFactory();

        return httpClientFactory.getClient( url, config, connection.getKerberosConnection() );
    }


    /**
     * @see UnregisteredDriver#getFactoryClassName(JdbcVersion)
     */
    @Override
    protected String getFactoryClassName( final JdbcVersion jdbcVersion ) {
        switch ( jdbcVersion ) {
            case JDBC_30:
            case JDBC_40:
                throw new IllegalArgumentException( "JDBC version not supported: " + jdbcVersion );

            case JDBC_41:
            default:
                return "org.polypheny.jdbc.PolyphenyJdbc41Factory";
        }
    }


    @Override
    public Connection connect( String url, Properties info ) throws SQLException {
        if ( url == null ) {
            throw new SQLException( new NullPointerException( "url == null" ) );
        }

        final AvaticaConnection connection;
        if ( url.toLowerCase().contains( "url=http://" ) || url.toLowerCase().contains( "url=https://" ) ) {
            // Avatica-compatible --    jdbc:polypheny:url=http(s)://server.address/database;...
            connection = (AvaticaConnection) super.connect( url, info );
        } else if ( url.toLowerCase().contains( getConnectStringPrefix().toLowerCase() + "http://" ) || url.toLowerCase().contains( getConnectStringPrefix().toLowerCase() + "https://" ) ) {
            // New Poly style --    jdbc:polypheny:http(s)://server.address/database&...
            info = parseUrl( url, info );
            if ( info == null ) {
                // Something is wrong with the url
                return null;
            }
            connection = (AvaticaConnection) super.connect( url, info );
        } else {
            // Old style -- jdbc:polypheny://server.address/database&...
            log.info( "No transport scheme given. Falling back to http. This might change in future." );
            info = parseUrl( url, info );
            if ( info == null ) {
                // Something is wrong with the url
                return null;
            }
            connection = (AvaticaConnection) super.connect( url, info );
        }
        if ( connection == null ) {
            // It's not an url for our driver
            return null;
        }

        final Service service = connection.getService();
        // super.connect(...) should be creating a service and setting it in the AvaticaConnection
        assert service != null;

        final OpenConnectionResponse response = service.apply( new OpenConnectionRequest( connection.id, OpenConnectionRequest.serializeProperties( info ) ) );
        if ( response == null ) {
            throw new SQLException( "Exception opening a connection. The response is `null`." );
        }

        return connection;
    }


    // packet-visible for testability
    @SuppressWarnings("deprecated")
    final Properties parseUrl( String url, final Properties defaults ) {
        final Properties prop = (defaults == null) ? new Properties() : new Properties( defaults );

        if ( url == null || url.isEmpty() ) {
            return null;
        }

        log.debug( "Parsing \"" + url + "\"" );

        final int questionMarkPosition = url.indexOf( '?' );
        if ( questionMarkPosition != -1 ) {
            // we have some parameters
            final String parameters = url.substring( questionMarkPosition + 1 );
            url = url.substring( 0, questionMarkPosition );

            final StringTokenizer parameterTokens = new StringTokenizer( parameters, "&" );
            while ( parameterTokens.hasMoreTokens() ) {
                String parameter = parameterTokens.nextToken();

                final int equalPosition = parameter.indexOf( '=' );
                String parameterKey = null;
                String parameterValue = null;

                if ( equalPosition != -1 ) {
                    parameterKey = parameter.substring( 0, equalPosition );

                    if ( equalPosition + 1 < parameter.length() ) {
                        parameterValue = parameter.substring( equalPosition + 1 );
                    }
                }

                if ( (parameterKey != null && parameterKey.length() > 0) && (parameterValue != null && parameterValue.length() > 0) ) {
                    try {
                        try {
                            prop.setProperty( parameterKey, URLDecoder.decode( parameterValue, StandardCharsets.UTF_8.name() ) );
                        } catch ( UnsupportedEncodingException e ) {
                            // not going to happen - value came from JDK's own StandardCharsets
                            throw new RuntimeException( e );
                        }
                    } catch ( NoSuchMethodError e ) {
                        log.debug( "Cannot use the decode method with UTF-8. Using the fallback (deprecated) method.", e );
                        //noinspection deprecation
                        prop.setProperty( parameterKey, URLDecoder.decode( parameterValue ) );
                    }
                }
            }
        }

        final int doubleSlashPosition = url.indexOf( "//" );
        if ( doubleSlashPosition == -1 ) {
            return null;
        }

        final String scheme;
        try {
            scheme = url.substring( 0, doubleSlashPosition ).substring( getConnectStringPrefix().length() );
        } catch ( IndexOutOfBoundsException e ) {
            return null;
        }

        url = url.substring( doubleSlashPosition + 2 );

        final int atPosition = url.indexOf( '@' );
        if ( atPosition != -1 ) {
            // we have username[:password]@...
            final String userPassword = url.substring( 0, atPosition );
            url = url.substring( atPosition + 1 );

            final int colonPosition = userPassword.indexOf( ':' );
            String username;
            String password = null;

            if ( colonPosition != -1 ) {
                username = userPassword.substring( 0, colonPosition );

                if ( colonPosition + 1 < userPassword.length() ) {
                    password = userPassword.substring( colonPosition + 1 );
                }
            } else {
                username = userPassword;
            }

            //noinspection ConstantConditions
            if ( username != null && username.length() > 0 ) {
                prop.setProperty( PROPERTY_USERNAME_KEY, username );
            }
            if ( password != null && password.length() > 0 ) {
                prop.setProperty( PROPERTY_PASSWORD_KEY, password );
            }
        }

        final int slashPosition = url.indexOf( '/' );
        String hostPort = url;

        if ( slashPosition != -1 ) {
            hostPort = url.substring( 0, slashPosition );
            String database = null;

            if ( slashPosition + 1 < url.length() ) {
                database = url.substring( slashPosition + 1 );
            }

            if ( database != null && database.length() > 0 ) {
                prop.setProperty( PROPERTY_DATABASE_KEY, database );
            }
        }

        final int colonPosition = hostPort.indexOf( ':' );

        String host = hostPort;
        String port = Integer.toString( DEFAULT_PORT );

        if ( colonPosition != -1 ) {
            host = hostPort.substring( 0, colonPosition );

            if ( host.isEmpty() ) {
                host = DEFAULT_HOST;
            }

            if ( colonPosition + 1 < hostPort.length() ) {
                port = hostPort.substring( colonPosition + 1 );
            }
        }

        prop.setProperty( PROPERTY_HOST_KEY, host );
        prop.setProperty( PROPERTY_PORT_KEY, port );

        prop.setProperty( PROPERTY_URL_KEY, (scheme.isEmpty() ? "http:" : scheme) + "//" + prop.getProperty( PROPERTY_HOST_KEY, DEFAULT_HOST ) + ":" + prop.getProperty( PROPERTY_PORT_KEY, Integer.toString( DEFAULT_PORT ) ) + "/" );

        // OVERRIDE URL BY DEFAULT
        if ( defaults != null ) {
            for ( final Object o : defaults.keySet() ) {
                final String key = o.toString();
                final String value = defaults.getProperty( key );
                prop.setProperty( key, value );
            }
        }

        log.debug( "Result of parsing: {}", prop );

        return prop;
    }
}

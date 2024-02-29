package org.polypheny.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.jdbc.properties.PropertyUtils;

@Slf4j
public class ConnectionString {

    @Getter
    private String host;
    @Getter
    private int port;
    private HashMap<String, String> parameters;


    public ConnectionString( String url ) throws SQLException {
        this.parameters = new HashMap<>();
        parseUrl( url );
    }


    public ConnectionString( String url, Properties parameters ) throws SQLException {
        this.parameters = importPropertiesMap( parameters );
        parseUrl( url );
    }


    public String getUser() {
        return parameters.get( PropertyUtils.getUSERNAME_KEY() );
    }


    private HashMap<String, String> importPropertiesMap( Properties properties ) {
        if ( properties == null ) {
            return new HashMap<>();
        }
        return properties.entrySet().stream().collect( Collectors.toMap(
                e -> String.valueOf( e.getKey() ),
                e -> String.valueOf( e.getValue() ),
                ( prev, next ) -> next, HashMap::new ) );
    }


    private void parseUrl( String url ) throws SQLException {
        if ( url == null ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.URL_PARSING_INVALID, "URL must no be null." );
        }
        if ( !url.startsWith( DriverProperties.getDRIVER_URL_SCHEMA() ) ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.URL_PARSING_INVALID, "Invalid driver schema." );
        }
        log.debug( "Parsing url: \"" + url + "\"" );
        /*
        final int parameterStartIndex = url.indexOf( "?" );
        // parameters present
        if ( parameterStartIndex != -1 ) {
            parseParameters( substringAfter( parameterStartIndex, url ) );
            url = substringBefore( parameterStartIndex, url );
        }
         */
        final int schemeSpecificPartStartIndex = url.indexOf( "//" );
        if ( schemeSpecificPartStartIndex == -1 ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.URL_PARSING_INVALID, "Invalid url format." );
        }
        // + 1 removes the second / in //
        //url = substringAfter( schemeSpecificPartStartIndex + 1, url )

        this.host = PropertyUtils.getDEFAULT_HOST();
        this.port = PropertyUtils.getDEFAULT_PORT();
        url = url.substring( schemeSpecificPartStartIndex );

        if ( url.equals( "//" ) ) {
            return;
        }

        try {
            URI uri = new URI( url );
            if ( uri.getQuery() != null ) {
                parseParameters( uri.getQuery() );
            }
            if ( uri.getHost() != null ) {
                this.host = uri.getHost();
            }
            if ( uri.getPort() != -1 ) {
                this.port = uri.getPort();
            }
            if ( uri.getUserInfo() != null ) {
                String[] userAndPassword = uri.getUserInfo().split( ":", 2 );
                this.parameters.put( PropertyUtils.getUSERNAME_KEY(), userAndPassword[0] );
                if ( userAndPassword.length > 1 ) {
                    this.parameters.put( PropertyUtils.getPASSWORD_KEY(), userAndPassword[1] );
                }
            }
            if ( !uri.getPath().isEmpty() && uri.getPath().length() > 1 ) {
                this.parameters.put( PropertyUtils.getNAMESPACE_KEY(), uri.getPath().substring( 1 ) ); // Leading /
            }
        } catch ( URISyntaxException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
        /*
        final int authorityStartIndex = url.indexOf( "@" );
        // user information present
        if ( authorityStartIndex != -1 ) {
            parseUserInfo( substringBefore( authorityStartIndex, url ) );
            url = substringAfter( authorityStartIndex, url );
        }
        final int pathStartIndex = url.indexOf( '/' );
        // namespace specified
        if ( pathStartIndex != -1 && pathStartIndex + 1 < url.length() ) {
            parseNamespace( substringAfter( pathStartIndex, url ) );
            url = substringBefore( pathStartIndex, url );
        }
        parseAuthority( url );
         */
    }


    private void parseAuthority( String authority ) throws SQLException {
        log.debug( "Parsing authority: \"" + authority + "\"" );
        String host = authority;
        String port = String.valueOf( PropertyUtils.getDEFAULT_PORT() );
        final int hostPortSeparatorIndex = authority.indexOf( ":" );
        if ( hostPortSeparatorIndex != -1 ) {
            host = substringBefore( hostPortSeparatorIndex, authority );
            if ( hostPortSeparatorIndex + 1 < authority.length() ) {
                port = substringAfter( hostPortSeparatorIndex, authority );
            }
        }
        if ( host.isEmpty() ) {
            host = PropertyUtils.getDEFAULT_HOST();
        }
        this.host = host;
        this.port = Integer.parseInt( port );
    }


    private void parseNamespace( String path ) {
        log.debug( "Parsing namespace: \"" + path + "\"" );
        if ( !path.isEmpty() ) {
            parameters.put( PropertyUtils.getNAMESPACE_KEY(), path );
        }
    }


    private void parseUserInfo( String userInformation ) throws SQLException {
        log.debug( "Parsing user info: \"" + userInformation + "\"" );
        final int firstColumnPosition = userInformation.indexOf( ':' );
        String username;
        if ( firstColumnPosition == -1 ) {
            // no password given
            username = substringBefore( userInformation.length(), userInformation );
            if ( username.isEmpty() ) {
                return;
            }
            parameters.put( PropertyUtils.getUSERNAME_KEY(), username );
            return;
        }
        username = substringBefore( firstColumnPosition, userInformation );
        if ( username.isEmpty() ) {
            return;
        }
        parameters.put( PropertyUtils.getUSERNAME_KEY(), username );
        String password = substringAfter( firstColumnPosition, userInformation );
        parameters.put( PropertyUtils.getPASSWORD_KEY(), password );
    }


    private String substringBefore( int index, String string ) {
        return string.substring( 0, index );
    }


    private String substringAfter( int index, String string ) {
        return string.substring( index + 1 );
    }


    private void parseParameters( String parameters ) throws SQLException {
        log.debug( "Parsing url parameters: \"" + parameters + "\"" );
        StringTokenizer tokenizer = new StringTokenizer( parameters, "&" );
        String[] keyValuePair;
        while ( tokenizer.hasMoreTokens() ) {
            keyValuePair = tokenizer.nextToken().split( "=" );
            if ( keyValuePair.length != 2 ) {
                throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.URL_PARSING_INVALID, "Invalid parameter format." );
            }
            if ( keyValuePair[0].isEmpty() || keyValuePair[1].isEmpty() ) {
                throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.URL_PARSING_INVALID, "Invalid parameter format." );
            }
            try {
                String value = URLDecoder.decode( keyValuePair[1], StandardCharsets.UTF_8.name() );
                this.parameters.put( keyValuePair[0], value );
            } catch ( UnsupportedEncodingException uee ) {
                // not going to happen - value came from JDK's own StandardCharsets
            }
        }
    }


    public String getTarget() {
        return host + ":" + port;
    }


    public HashMap<String, String> getParameters() {
        String property = parameters.get( PropertyUtils.getPASSWORD_KEY() );
        if ( property != null && property.equals( DriverProperties.getBACKDOR_STRING() ) && DriverProperties.isBACKDOOR_ENABLED() ) {
            parameters.put( PropertyUtils.getPASSWORD_KEY(), "" );
        }
        return parameters;
    }


    public String getParameter( String parameterName ) {
        return parameters.get( parameterName );
    }

}

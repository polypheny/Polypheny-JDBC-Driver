package org.polypheny.jdbc;

import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;

@Slf4j
public class ConnectionString {
    private String target;
    private HashMap<String, String> parameters;

    public ConnectionString(String url) throws SQLException {
        this.parameters = new HashMap<>();
        parseUrl(url);
    }

    public ConnectionString(String url, Properties parameters) throws SQLException {
        this.parameters = importPropertiesMap(parameters);
        parseUrl(url);
    }

    private HashMap<String, String> importPropertiesMap(Properties properties) {
        HashMap<String, String> map = new HashMap<>();
        if (properties == null) {
            log.error("Properties map is null.");
            return map;
        }
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            map.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        return map;
    }

    private void parseUrl(String url) throws SQLException {
        if (url == null) {
            throw new SQLException("URL must no be null.");
        }
        if (!url.startsWith(PolyphenyDriver.DRIVER_URL_SCHEMA)) {
            throw new SQLException("Invalid driver schema.");
        }
        log.debug("Parsing url: \"" + url + "\"");
        final int parameterStartIndex = url.indexOf("?");
        // parameters present
        if (parameterStartIndex != -1) {
            parseParameters(substringAfter(parameterStartIndex, url));
            url = substringBefore(parameterStartIndex, url);
        }
        final int schemeSpecificPartStartIndex = url.indexOf("//");
        if (schemeSpecificPartStartIndex == -1) {
            throw new SQLException("Invalid url format.");
        }
        // + 1 removes the second / in //
        url = substringAfter(schemeSpecificPartStartIndex + 1, url);
        final int authorityStartIndex = url.indexOf("@");
        // user information present
        if (authorityStartIndex != -1) {
            parseUserInfo(substringBefore(authorityStartIndex, url));
            url = substringAfter(authorityStartIndex, url);
        }
        final int pathStartIndex = url.indexOf('/');
        // namespace specified
        if (pathStartIndex != -1 && pathStartIndex + 1 < url.length()) {
            parseNamespace(substringAfter(pathStartIndex, url));
            url = substringBefore(pathStartIndex, url);
        }
        parseAuthority(url);
    }

    private void parseAuthority(String authority) throws SQLException {
        log.debug("Parsing authority: \"" + authority + "\"");
        String host = authority;
        String port = String.valueOf(PolyphenyDriver.DEFAULT_PORT);
        final int hostPortSeparatorIndex = authority.indexOf(":");
        if (hostPortSeparatorIndex != -1) {
            host = substringBefore(hostPortSeparatorIndex, authority);
            if (hostPortSeparatorIndex + 1 < authority.length()) {
                port = substringAfter(hostPortSeparatorIndex, authority);
            }
        }
        if (host.isEmpty()) {
            host = PolyphenyDriver.DEFAULT_HOST;
        }
        target = host + ":" + port;
    }

    private void parseNamespace(String path) {
        log.debug("Parsing namespace: \"" + path + "\"");
        if (!path.isEmpty()) {
            parameters.put(PolyphenyDriver.PROPERTY_NAMESPACE_KEY, path);
        }
    }

    private void parseUserInfo(String userInformation) throws SQLException {
        log.debug("Parsing user info: \"" + userInformation + "\"");
        String[] usernameAndPassword = userInformation.split(":");
        if (usernameAndPassword.length > 2) {
            throw new SQLException("Invalid user information format.");
        }
        if (usernameAndPassword[0].isEmpty()) {
            return;
        }
        parameters.put(PolyphenyDriver.PROPERTY_USERNAME_KEY, usernameAndPassword[0]);
        if (usernameAndPassword.length == 1 || usernameAndPassword[1].isEmpty()) {
            return;
        }
        parameters.put(PolyphenyDriver.PROPERTY_PASSWORD_KEY, usernameAndPassword[1]);
    }

    private String substringBefore(int index, String string) {
        return string.substring(0, index);
    }

    private String substringAfter(int index, String string) {
        return string.substring(index + 1);
    }

    private void parseParameters(String parameters) throws SQLException {
        log.debug("Parsing url parameters: \"" + parameters + "\"");
        StringTokenizer tokenizer = new StringTokenizer(parameters, "&");
        String[] keyValuePair;
        while (tokenizer.hasMoreTokens()) {
            keyValuePair = tokenizer.nextToken().split("=");
            if (keyValuePair.length != 2) {
                throw new SQLException("Invalid parameter format.");
            }
            if (keyValuePair[0].isEmpty() || keyValuePair[1].isEmpty()) {
                throw new SQLException("Invalid parameter format.");
            }
            try {
                String value = URLDecoder.decode(keyValuePair[1], StandardCharsets.UTF_8.name());
                this.parameters.put(keyValuePair[0], value);
            } catch (UnsupportedEncodingException uee) {
                // not going to happen - value came from JDK's own StandardCharsets
            }
        }
    }

    public String getTarget() {
        return target;
    }

    public HashMap<String, String> getParameters() {
        return parameters;
    }
}

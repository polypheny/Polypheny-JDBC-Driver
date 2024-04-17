/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.jdbc.properties;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Getter;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class PropertyUtils {

    // Default values for JDBC properties.
    @Getter
    private static final int DEFAULT_TRANSACTION_ISOLATION = Connection.TRANSACTION_READ_COMMITTED;
    @Getter
    private static final int DEFAULT_NETWORK_TIMEOUT = 0;
    @Getter
    private static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 0;
    @Getter
    private static final int DEFAULT_FETCH_SIZE = 100;
    @Getter
    private static final int DEFAULT_FETCH_DIRECTION = ResultSet.FETCH_FORWARD;
    @Getter
    private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    @Getter
    private static final int DEFAULT_RESULTSET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    @Getter
    private static final int DEFAULT_MAX_FIELD_SIZE = 0;
    @Getter
    private static final long DEFAULT_LARGE_MAX_ROWS = 0;
    @Getter
    private static final boolean DEFAULT_DOING_ESCAPE_PROCESSING = true;
    @Getter
    // Pooling not supported. Default still needed for various jdbc methods.
    private static final boolean DEFAULT_STATEMENT_POOLABLE = false;
    @Getter
    // Pooling not supported. Default still needed for various jdbc methods.
    private static final boolean DEFAULT_PREPARED_STATEMENT_POOLABLE = false;
    @Getter
    // Pooling not supported. Default still needed for various jdbc methods.
    private static final boolean DEFAULT_CALLABLE_STATEMENT_POOLABLE = false;
    @Getter
    private static final boolean DEFAULT_AUTOCOMMIT = true;
    @Getter
    private static final boolean DEFAULT_READ_ONLY = false;
    @Getter
    private static final int DEFAULT_RESULTSET_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    @Getter
    private static final String DEFAULT_HOST = "localhost";
    @Getter
    private static final int DEFAULT_PORT = 20590;
    @Getter
    private static final String SQL_LANGUAGE_NAME = "sql";
    // Keys for properties
    @Getter
    private static final String USERNAME_KEY = "user";
    @Getter
    @java.lang.SuppressWarnings(
            "squid:S2068"
            // Credentials should not be hard-coded: 'password' detected
            // Justification: "password" is here the key to set the password in the connection parameters.
    )
    private static final String PASSWORD_KEY = "password";
    @Getter
    private static final String NAMESPACE_KEY = "namespace";
    @Getter
    private static final String AUTOCOMMIT_KEY = "autocommit";
    @Getter
    private static final String READ_ONLY_KEY = "readonly";
    @Getter
    private static final String RESULT_SET_HOLDABILITY_KEY = "holdability";
    @Getter
    private static final String NETWORK_TIMEOUT_KEY = "nwtimeout";
    @Getter
    private static final String TRANSACTION_ISOLATION_KEY = "isolation";
    @Getter
    private static final String TIMEZONE_KEY = "timezone";
    @Getter
    private static final String STRICT_MODE_KEY = "strict";


    public static String getHoldabilityName( int resultSetHoldability ) throws PrismInterfaceServiceException {
        switch ( resultSetHoldability ) {
            case ResultSet.CLOSE_CURSORS_AT_COMMIT:
                return "CLOSE";
            case ResultSet.HOLD_CURSORS_OVER_COMMIT:
                return "HOLD";
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "The passed integer value does not match a result holdability." );
    }


    public static String getTransactionIsolationName( int transactionIsolation ) throws PrismInterfaceServiceException {
        switch ( transactionIsolation ) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return "DIRTY";
            case Connection.TRANSACTION_READ_COMMITTED:
                return "COMMITTED";
            case Connection.TRANSACTION_SERIALIZABLE:
                return "SERIALIZABLE";
            case Connection.TRANSACTION_REPEATABLE_READ:
                return "REPEATABLE_READ";
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "The passed integer value does not match a transaction isolation level." );
    }


    // Methods for input checking
    private static Map<Integer, List<Integer>> SUPPORTED_CONCURRENCIES = new HashMap<>();


    static {
        SUPPORTED_CONCURRENCIES.put( ResultSet.TYPE_FORWARD_ONLY, Collections.singletonList( ResultSet.CONCUR_READ_ONLY ) );
        SUPPORTED_CONCURRENCIES.put( ResultSet.TYPE_SCROLL_INSENSITIVE, Collections.singletonList( ResultSet.CONCUR_READ_ONLY ) );
    }


    private static final Set<Integer> RESULT_SET_TYPES = new HashSet<>();


    static {
        RESULT_SET_TYPES.add( ResultSet.TYPE_FORWARD_ONLY );
        RESULT_SET_TYPES.add( ResultSet.TYPE_SCROLL_INSENSITIVE );
    }


    private static final Set<Integer> RESULT_SET_CONCURRENCIES = new HashSet<>();


    static {
        RESULT_SET_CONCURRENCIES.add( ResultSet.CONCUR_READ_ONLY );
        RESULT_SET_CONCURRENCIES.add( ResultSet.CONCUR_UPDATABLE );
    }


    private static final Set<Integer> RESULT_SET_HOLDABILITIES = new HashSet<>();


    static {
        RESULT_SET_HOLDABILITIES.add( ResultSet.CLOSE_CURSORS_AT_COMMIT );
    }


    private static final Set<Integer> TRANSACTION_ISOLATION_LEVELS = new HashSet<>();


    static {
        TRANSACTION_ISOLATION_LEVELS.add( Connection.TRANSACTION_READ_COMMITTED );
    }


    private static final Set<Integer> AUTO_GENERATED_KEYS = new HashSet<>();


    static {
        AUTO_GENERATED_KEYS.add( Statement.RETURN_GENERATED_KEYS );
        AUTO_GENERATED_KEYS.add( Statement.NO_GENERATED_KEYS );
    }


    private static final Set<Integer> FETCH_DIRECTIONS = new HashSet<>();


    static {
        FETCH_DIRECTIONS.add( ResultSet.FETCH_FORWARD );
    }


    public static boolean isValidResultSetConcurrency( int resultSetType, int resultSeteConcurrency ) {
        List<Integer> supportedConcurrencies = SUPPORTED_CONCURRENCIES.get( resultSetType );
        if ( supportedConcurrencies == null ) {
            return false;
        }
        return supportedConcurrencies.contains( resultSeteConcurrency );
    }


    public static boolean isValidResultSetType( int resultSetType ) {
        return RESULT_SET_TYPES.contains( resultSetType );
    }


    public static boolean isValidResultSetConcurrency( int resultSetConcurrency ) {
        return RESULT_SET_CONCURRENCIES.contains( resultSetConcurrency );
    }


    public static boolean isValidResultSetHoldability( int resultSetHoldability ) {
        return RESULT_SET_HOLDABILITIES.contains( resultSetHoldability );
    }


    public static boolean isValidIsolationLevel( int transacitonIsolationLevel ) {
        return TRANSACTION_ISOLATION_LEVELS.contains( transacitonIsolationLevel );
    }


    public static void throwIfInvalid( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfInvalid( resultSetType, resultSetConcurrency );
        if ( !isValidResultSetHoldability( resultSetHoldability ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set holdability." );
        }
    }


    public static void throwIfInvalid( int resultSetType, int resultSetConcurrency ) throws SQLException {
        if ( !isValidResultSetType( resultSetType ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set type." );
        }
        if ( !isValidResultSetConcurrency( resultSetConcurrency ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal value for result set concurrency." );
        }
        if ( !isValidResultSetConcurrency( resultSetType, resultSetConcurrency ) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPTION_NOT_SUPPORTED, "The specified concurrency is not supported for the specified result set type" );
        }
    }


    public static boolean isValidAutogeneratedKeys( int autogeneratedKeys ) {
        return AUTO_GENERATED_KEYS.contains( autogeneratedKeys );
    }


    public static boolean isInvalidFetchDdirection( int fetchDirection ) {
        return !FETCH_DIRECTIONS.contains( fetchDirection );
    }

}

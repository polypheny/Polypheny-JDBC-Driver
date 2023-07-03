package org.polypheny.jdbc.utils;

import com.google.common.collect.ImmutableSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import lombok.Getter;

public class PropertyUtils {

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
    private static final boolean DEFAULT_STATEMENT_POOLABLE = false;
    @Getter
    private static final boolean DEFAULT_PREPARED_STATEMENT_POOLABLE = true;
    @Getter
    private static final boolean DEFAULT_CALLABLE_STATEMENT_POOLABLE = true;
    @Getter
    private static final boolean DEFAULT_AUTOCOMMIT = true;
    @Getter
    private static final boolean DEFAULT_READ_ONLY = false;
    @Getter
    private static final int DEFAULT_RESULTSET_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;


    @Getter
    private static final String DEFAULT_HOST = "localhost";
    @Getter
    private static final int DEFAULT_PORT = 20591;

    @Getter
    public static final String USERNAME_KEY = "user";
    @Getter
    @java.lang.SuppressWarnings(
            "squid:S2068"
            // Credentials should not be hard-coded: 'password' detected
            // Justification: "password" is here the key to set the password in the connection parameters.
    )
    public static final String PASSWORD_KEY = "password";
    public static final String NAMESPACE_KEY = "namespace";

    private static final Set<Integer> RESULT_SET_TYPES = ImmutableSet.<Integer>builder()
            .add( ResultSet.TYPE_FORWARD_ONLY )
            .add( ResultSet.TYPE_SCROLL_INSENSITIVE )
            .add( ResultSet.TYPE_SCROLL_SENSITIVE )
            .build();

    private static final Set<Integer> RESULT_SET_CONCURRENCIES = ImmutableSet.<Integer>builder()
            .add( ResultSet.CONCUR_READ_ONLY )
            .add( ResultSet.CONCUR_UPDATABLE )
            .build();

    private static final Set<Integer> RESULT_SET_HOLDABILITIES = ImmutableSet.<Integer>builder()
            .add( ResultSet.HOLD_CURSORS_OVER_COMMIT )
            .add( ResultSet.CLOSE_CURSORS_AT_COMMIT )
            .build();

    private static final Set<Integer> TRANSACTION_ISOLATION_LEVELS = ImmutableSet.<Integer>builder()
            .add( Connection.TRANSACTION_READ_COMMITTED )
            .add( Connection.TRANSACTION_READ_UNCOMMITTED )
            .add( Connection.TRANSACTION_SERIALIZABLE )
            .add( Connection.TRANSACTION_REPEATABLE_READ )
            .build();

    private static final Set<Integer> AUTO_GENERATED_KEYS = ImmutableSet.<Integer>builder()
            .add( Statement.NO_GENERATED_KEYS )
            .add( Statement.RETURN_GENERATED_KEYS )
            .build();

    private static final Set<Integer> FETCH_DIRECTIONS = ImmutableSet.<Integer>builder()
            .add( ResultSet.FETCH_FORWARD )
            .add( ResultSet.FETCH_REVERSE )
            .add( ResultSet.FETCH_UNKNOWN )
            .build();


    public static boolean isValidResultSetType( int resultSetType ) {
        return RESULT_SET_TYPES.contains( resultSetType );
    }


    public static boolean isValidResultSetConcurrency( int resultSetConcurrency ) {
        return RESULT_SET_CONCURRENCIES.contains( resultSetConcurrency );
    }


    public static boolean isInvalidResultSetHoldability( int resultSetHoldability ) {
        return !RESULT_SET_HOLDABILITIES.contains( resultSetHoldability );
    }


    public static boolean isValidIsolationLevel( int transacitonIsolationLevel ) {
        return TRANSACTION_ISOLATION_LEVELS.contains( transacitonIsolationLevel );
    }


    public static void throwIfOneInvalid( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        throwIfOneInvalid( resultSetType, resultSetConcurrency );
        if ( isInvalidResultSetHoldability( resultSetHoldability ) ) {
            throw new SQLException( "Illegal value for result set holdability." );
        }
    }


    public static void throwIfOneInvalid( int resultSetType, int resultSetConcurrency ) throws SQLException {
        if ( !isValidResultSetType( resultSetType ) ) {
            throw new SQLException( "Illegal value for result set type." );
        }
        if ( !isValidResultSetConcurrency( resultSetConcurrency ) ) {
            throw new SQLException( "Illegal value for result set concurrency." );
        }
    }


    public static boolean isValidAutogeneratedKeys( int autogeneratedKeys ) {
        return AUTO_GENERATED_KEYS.contains( autogeneratedKeys );
    }


    public static boolean isInvalidFetchDdirection( int fetchDirection ) {
        return !FETCH_DIRECTIONS.contains( fetchDirection );
    }

}
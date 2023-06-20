package org.polypheny.jdbc.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import javax.xml.transform.Result;
import lombok.Getter;

public class DefaultPropertyValues {
    @Getter
    private static final int DEFAULT_TRANSACTION_ISOLATION = Connection.TRANSACTION_READ_COMMITTED;
    @Getter
    private static final int DEFAULT_NETWORK_TIMEOUT = 0;
    @Getter
    private static final int QUERY_TIMEOUT_SECONDS = 0;
    @Getter
    private static final int FETCH_SIZE = 100;
    @Getter
    private static final int FETCH_DIRECTION = ResultSet.FETCH_FORWARD;
    @Getter
    private static final int DEFAULT_RESULTSET_TYPE = ResultSet.TYPE_FORWARD_ONLY;
    @Getter
    private static final int DEFAULT_RESULTSET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;
    @Getter
    private static final int MAX_FIELD_SIZE = 0;
    @Getter
    private static final int MAX_ROWS = 0;
    @Getter
    private static final long LARGE_MAX_ROWS = 0;
    @Getter
    private static final boolean DOING_ESCAPE_PROCESSING = true;
    @Getter
    private static final boolean STATEMENT_POOLABLE = false;
    @Getter
    private static final boolean PREPARED_STATEMENT_POOLABLE = true;
    @Getter
    private static final boolean CALLABLE_STATEMENT_POOLABLE = true;
    @Getter
    private static final boolean AUTOCOMMIT = true;
    @Getter
    private static final boolean READ_ONLY = false;
    @Getter
    private static final int DEFAULT_RESULTSET_HOLDABILITY = ResultSet.CLOSE_CURSORS_AT_COMMIT;

}

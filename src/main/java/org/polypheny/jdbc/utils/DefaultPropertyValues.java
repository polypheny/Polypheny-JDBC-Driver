package org.polypheny.jdbc.utils;

import java.sql.ResultSet;
import lombok.Getter;

public class DefaultPropertyValues {

    @Getter
    private static final int QUERY_TIMEOUT_SECONDS = 0;
    @Getter
    private static final int FETCH_SIZE = 100;
    @Getter
    private static final int FETCH_DIRECTION = ResultSet.FETCH_FORWARD;
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

}

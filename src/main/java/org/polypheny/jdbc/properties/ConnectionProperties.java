package org.polypheny.jdbc.properties;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.StatementProperties;

public class ConnectionProperties {

    public ConnectionProperties() {
        this.isAutoCommit = PropertyUtils.isDEFAULT_AUTOCOMMIT();
        this.isReadOnly = PropertyUtils.isDEFAULT_READ_ONLY();
        this.resultSetHoldability = PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY();
        this.networkTimeout = PropertyUtils.getDEFAULT_NETWORK_TIMEOUT();
        this.transactionIsolation = PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION();
    }


    @Getter
    @Setter
    private boolean isAutoCommit;
    @Getter
    @Setter
    private boolean isReadOnly;
    @Getter
    @Setter
    private int resultSetHoldability;
    @Getter
    @Setter
    private int networkTimeout;
    @Getter
    @Setter
    private int transactionIsolation;

    @Getter
    @Setter
    private String catalogName;


    public StatementProperties toStatementProperties() {
        return toStatementProperties(
                PropertyUtils.getDEFAULT_RESULTSET_TYPE(),
                PropertyUtils.getDEFAULT_RESULTSET_CONCURRENCY()
        );
    }


    public StatementProperties toStatementProperties( int resultSetType, int resultSetConcurrency ) {
        return toStatementProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    public StatementProperties toStatementProperties( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        StatementProperties properties = new StatementProperties();
        properties.setQueryTimeoutSeconds( PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS() );
        properties.setResultSetType( resultSetType );
        properties.setResultSetConcurrency( resultSetConcurrency );
        properties.setResultSetHoldability( resultSetHoldability );
        properties.setFetchSize( PropertyUtils.getDEFAULT_FETCH_SIZE() );
        properties.setFetchDirection( PropertyUtils.getDEFAULT_FETCH_DIRECTION() );
        properties.setMaxFieldSize( PropertyUtils.getDEFAULT_MAX_FIELD_SIZE() );
        properties.setLargeMaxRows( PropertyUtils.getDEFAULT_LARGE_MAX_ROWS() );
        properties.setDoesEscapeProcessing( PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING() );
        properties.setPoolable( PropertyUtils.isDEFAULT_STATEMENT_POOLABLE() );
        return properties;
    }

}

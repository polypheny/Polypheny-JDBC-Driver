package org.polypheny.jdbc;

import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.utils.DefaultPropertyValues;

public class ConnectionProperties {

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
    private boolean hasRunningTransaction;


    public StatementProperties toStatementProperties() {
        return toStatementProperties(
                DefaultPropertyValues.getDEFAULT_RESULTSET_TYPE(),
                DefaultPropertyValues.getDEFAULT_RESULTSET_CONCURRENCY()
        );
    }


    public StatementProperties toStatementProperties( int resultSetType, int resultSetConcurrency ) {
        return toStatamentProperties( resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    public StatementProperties toStatamentProperties( int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        StatementProperties properties = new StatementProperties();
        properties.setQueryTimeoutSeconds( DefaultPropertyValues.getQUERY_TIMEOUT_SECONDS() );
        properties.setResultSetType( resultSetType );
        properties.setResultSetConcurrency( resultSetConcurrency );
        properties.setResultSetHoldability( resultSetHoldability );
        properties.setFetchSize( DefaultPropertyValues.getFETCH_SIZE() );
        properties.setFetchDirection( DefaultPropertyValues.getFETCH_DIRECTION() );
        properties.setMaxFieldSize( DefaultPropertyValues.getMAX_FIELD_SIZE() );
        properties.setMaxRows( DefaultPropertyValues.getMAX_ROWS() );
        properties.setLargeMaxRows( DefaultPropertyValues.getLARGE_MAX_ROWS() );
        properties.setDoesEscapeProcessing( DefaultPropertyValues.isDOING_ESCAPE_PROCESSING() );
        properties.setPoolable( DefaultPropertyValues.isSTATEMENT_POOLABLE() );
        return properties;
    }

}

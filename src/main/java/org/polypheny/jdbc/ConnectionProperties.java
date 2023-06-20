package org.polypheny.jdbc;

public class ConnectionProperties {
    private boolean isAutoCommit;
    private boolean isReadOnly;
    private int resultSetHoldability;
    private int networkTimeout;
    private int transactionIsolation;

    private boolean hasRunningTransaction;

}

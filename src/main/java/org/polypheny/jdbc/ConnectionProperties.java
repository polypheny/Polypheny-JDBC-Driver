package org.polypheny.jdbc;

import lombok.Getter;
import lombok.Setter;

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

}

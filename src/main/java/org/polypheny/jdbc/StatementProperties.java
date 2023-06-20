package org.polypheny.jdbc;

import lombok.Getter;
import lombok.Setter;

public class StatementProperties {
    @Getter
    @Setter
    private int queryTimeoutSeconds;
    @Getter
    @Setter
    private int resultSetType;
    @Getter
    @Setter
    private int resultSetConcurrency;
    @Getter
    @Setter
    private int resultSetHoldability;
    @Getter
    @Setter
    private int fetchSize;
    @Getter
    @Setter
    private int fetchDirection;
    @Getter
    @Setter
    private int maxFieldSize;
    @Getter
    @Setter
    private int maxRows;
    @Getter
    @Setter
    private long largeMaxRows;
    @Getter
    @Setter
    private boolean doesEscapeProcessing;
    @Getter
    @Setter
    private boolean isPoolable;
    @Getter
    @Setter
    private boolean isClosed;
    @Getter
    @Setter
    private boolean isClosedOnCompletion;

}

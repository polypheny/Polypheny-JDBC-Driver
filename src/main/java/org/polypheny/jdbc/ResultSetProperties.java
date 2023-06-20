package org.polypheny.jdbc;

import lombok.Getter;
import lombok.Setter;

public class ResultSetProperties {
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
    private int fetchDirection;
    @Getter
    @Setter
    private int fetchSize;
    @Getter
    @Setter
    private int maxFieldSize;
    @Getter
    @Setter
    private int maxRows;
    @Getter
    @Setter
    private long largeMaxRows;
}

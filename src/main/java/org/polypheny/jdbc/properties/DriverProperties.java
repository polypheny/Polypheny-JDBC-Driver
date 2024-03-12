package org.polypheny.jdbc.properties;

import java.util.TimeZone;
import lombok.Getter;

public class DriverProperties {

    @Getter
    private static final String DRIVER_NAME = "JDBC driver for PolyphenyDB";
    @Getter
    //TODO TH can we automate this?
    private static final int DRIVER_MAJOR_VERSION = 2;
    @Getter
    //TODO TH can we automate this?
    private static final int DRIVER_MINOR_VERSION = 0;
    @Getter
    //TODO TH can we automate this?
    private static final String DRIVER_VERSION_QUALIFIER = "-SNAPSHOT";
    @Getter
    private static final String DRIVER_VERSION = DRIVER_MAJOR_VERSION + '.' + DRIVER_MINOR_VERSION + DRIVER_VERSION_QUALIFIER;
    @Getter
    private static final boolean JDBC_COMPLIANT = false;
    @Getter
    private static final String DRIVER_URL_SCHEMA = "jdbc:polypheny:";
    @Getter
    private static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();
    @Getter
    // This feature is for testing purposes only!
    private static final boolean BACKDOOR_ENABLED = true;
    @Getter
    private static final String BACKDOR_STRING = "dasKannKeinEmptyString";

}

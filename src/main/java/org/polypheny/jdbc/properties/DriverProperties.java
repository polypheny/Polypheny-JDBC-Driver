/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
    // This feature is for testing purposes only! Always set to false before release!
    private static final boolean BACKDOOR_ENABLED = false;
    @Getter
    private static final String BACKDOR_STRING = "dasKannKeinEmptyString";

}

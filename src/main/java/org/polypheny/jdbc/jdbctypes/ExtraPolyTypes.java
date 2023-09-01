/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.jdbc.jdbctypes;

public interface ExtraPolyTypes {

    // From JDK 1.6
    int ROWID = -8;
    int NCHAR = -15;
    int NVARCHAR = -9;
    int LONGNVARCHAR = -16;
    int NCLOB = 2011;
    int SQLXML = 2009;

    // From JDK 1.8
    int REF_CURSOR = 2012;
    int TIME_WITH_TIMEZONE = 2013;
    int TIMESTAMP_WITH_TIMEZONE = 2014;

    // From OpenGIS
    int GEOMETRY = 2015; // TODO: confirm
}
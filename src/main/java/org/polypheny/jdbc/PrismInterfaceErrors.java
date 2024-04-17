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

package org.polypheny.jdbc;

public enum PrismInterfaceErrors {
    UNSPECIFIED( "UNSPECIFIED", -1 ),
    DRIVER_THREADING_ERROR( "I1001", 1 ),
    URL_PARSING_INVALID( "I2001", 2 ),
    RESULT_TYPE_INVALID( "I3001", 3 ),
    COLUMN_NOT_EXISTS( "42S22", 4 ),
    COLUMN_ACCESS_ILLEGAL( "22003", 5 ),
    OPERATION_ILLEGAL( "42000", 6 ),
    MODIFICATION_NOT_PERMITED( "2F002", 7 ),
    VALUE_ILLEGAL( "22003", 8 ),
    STREAM_ERROR( "I4001", 9 ),
    WRAPPER_INCORRECT_TYPE( "I5001", 10 ),
    CONNECTION_LOST( "08003", 11 ),
    UDT_REACHED_END( "I4002", 12 ),
    PARAMETER_NOT_EXISTS( "42000", 13 ),
    OPTION_NOT_SUPPORTED( "0A000", 14 ),
    DATA_TYPE_MISSMATCH( "42S22", 17 ),
    MISSING_INTERFACE( "I4003", 18 ),
    UDT_CONSTRUCTION_FAILED( "I4003", 19 ),
    ENTRY_NOT_EXISTS( "I5001", 20 );


    public final String state;
    public final int errorCode;


    PrismInterfaceErrors( String state, int errorCode ) {
        this.state = state;
        this.errorCode = errorCode;
    }
}

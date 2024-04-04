package org.polypheny.jdbc;

import java.sql.SQLException;
import org.polypheny.db.protointerface.proto.ErrorDetails;

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
public class PrismInterfaceServiceException extends SQLException {

    public PrismInterfaceServiceException( PrismInterfaceErrors sqlError, String message ) {
        this( message, sqlError.state, sqlError.errorCode );
    }

    public PrismInterfaceServiceException( PrismInterfaceErrors sqlError, String message, Throwable cause ) {
        this( message, sqlError.state, sqlError.errorCode, cause );
    }


    public PrismInterfaceServiceException( String reason, String state, int errorCode ) {
        super( reason, state, errorCode );
    }


    public PrismInterfaceServiceException( String reason, String state ) {
        super( reason, state );

    }


    public PrismInterfaceServiceException( String reason ) {
        super( reason, PrismInterfaceErrors.UNSPECIFIED.state, PrismInterfaceErrors.UNSPECIFIED.errorCode );
    }


    public PrismInterfaceServiceException() {
        super();
    }


    public PrismInterfaceServiceException( Throwable cause ) {
        super( cause.getMessage(), PrismInterfaceErrors.UNSPECIFIED.state, PrismInterfaceErrors.UNSPECIFIED.errorCode, cause );
    }


    public PrismInterfaceServiceException( String reason, Throwable cause ) {
        super( reason, cause );
    }


    public PrismInterfaceServiceException( String reason, String state, Throwable cause ) {
        super( reason, state, cause );
    }


    public PrismInterfaceServiceException( String reason, String state, int errorCode, Throwable cause ) {
        super( reason, state, errorCode, cause );
    }


    public PrismInterfaceServiceException( ErrorDetails errorDetails ) {
        super(
                errorDetails.hasMessage() ? errorDetails.getMessage() : "No message provided.",
                errorDetails.hasState() ? errorDetails.getState() : PrismInterfaceErrors.UNSPECIFIED.state,
                errorDetails.hasErrorCode() ? errorDetails.getErrorCode() : PrismInterfaceErrors.UNSPECIFIED.errorCode
        );
    }

}

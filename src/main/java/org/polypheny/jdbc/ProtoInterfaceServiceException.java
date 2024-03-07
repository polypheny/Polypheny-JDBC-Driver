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
public class ProtoInterfaceServiceException extends SQLException {

    public ProtoInterfaceServiceException( ProtoInterfaceErrors sqlError, String message ) {
        this( message, sqlError.state, sqlError.errorCode );
    }


    public ProtoInterfaceServiceException( ProtoInterfaceErrors sqlError, String message, Throwable cause ) {
        this( message, sqlError.state, sqlError.errorCode, cause );
    }


    public ProtoInterfaceServiceException( String reason, String state, int errorCode ) {
        super( reason, state, errorCode );
    }


    public ProtoInterfaceServiceException( String reason, String state ) {
        super( reason, state );

    }


    public ProtoInterfaceServiceException( String reason ) {
        super( reason, ProtoInterfaceErrors.UNSPECIFIED.state, ProtoInterfaceErrors.UNSPECIFIED.errorCode );
    }


    public ProtoInterfaceServiceException() {
        super();
    }


    public ProtoInterfaceServiceException( Throwable cause ) {
        super( cause.getMessage(), ProtoInterfaceErrors.UNSPECIFIED.state, ProtoInterfaceErrors.UNSPECIFIED.errorCode, cause );
    }


    public ProtoInterfaceServiceException( String reason, Throwable cause ) {
        super( reason, cause );
    }


    public ProtoInterfaceServiceException( String reason, String state, Throwable cause ) {
        super( reason, state, cause );
    }


    public ProtoInterfaceServiceException( String reason, String state, int errorCode, Throwable cause ) {
        super( reason, state, errorCode, cause );
    }


    public ProtoInterfaceServiceException( ErrorDetails errorDetails ) {
        super(
                errorDetails.hasMessage() ? errorDetails.getMessage() : "No message provided.",
                errorDetails.hasState() ? errorDetails.getState() : ProtoInterfaceErrors.UNSPECIFIED.state,
                errorDetails.hasErrorCode() ? errorDetails.getErrorCode() : ProtoInterfaceErrors.UNSPECIFIED.errorCode
        );
    }

}

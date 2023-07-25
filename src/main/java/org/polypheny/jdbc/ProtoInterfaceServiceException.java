package org.polypheny.jdbc;

import io.grpc.Metadata;
import io.grpc.protobuf.ProtoUtils;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Optional;
import org.polypheny.jdbc.proto.ErrorDetails;

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
public class ProtoInterfaceServiceException extends SQLException{
    private static final String STATE_UNSPECIFIED = "UNKNOWN";
    private static final int ERROR_UNSPECIFIED = -1;

    public static final Metadata.Key<ErrorDetails> ERROR_DETAILS_KEY = ProtoUtils.keyForProto( ErrorDetails.getDefaultInstance() );

    public static ProtoInterfaceServiceException fromMetadata(String message, Metadata metadata) throws ProtoInterfaceServiceException {
        if (metadata == null) {
            throw new ProtoInterfaceServiceException(message);
        }
        if (!metadata.containsKey( ERROR_DETAILS_KEY )) {
            throw new ProtoInterfaceServiceException(message, STATE_UNSPECIFIED, ERROR_UNSPECIFIED );
        }
        ErrorDetails errorDetails = metadata.get( ERROR_DETAILS_KEY );
        return new ProtoInterfaceServiceException( Objects.requireNonNull( errorDetails ));
    }

    public ProtoInterfaceServiceException( String reason, String state, int errorCode ) {
        super( reason, state, errorCode );
    }


    public ProtoInterfaceServiceException( String reason, String state ) {
        super( reason, state );

    }


    public ProtoInterfaceServiceException( String reason ) {
        super( reason );
    }


    public ProtoInterfaceServiceException() {
        super();
    }


    public ProtoInterfaceServiceException( Throwable cause ) {
        super( cause );
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
                errorDetails.hasMessage() ? errorDetails.getMessage() : null,
                errorDetails.hasState() ? errorDetails.getState() : STATE_UNSPECIFIED,
                errorDetails.hasErrorCode() ? errorDetails.getErrorCode() : ERROR_UNSPECIFIED
        );
    }


    public ErrorDetails getProtoErrorDetails() {
        ErrorDetails.Builder errorDetailsBuilder = ErrorDetails.newBuilder();
        errorDetailsBuilder.setErrorCode( getErrorCode() );
        Optional.ofNullable( getSQLState() ).ifPresent( errorDetailsBuilder::setState );
        Optional.ofNullable( getMessage() ).ifPresent( errorDetailsBuilder::setMessage );
        return errorDetailsBuilder.build();
    }
}

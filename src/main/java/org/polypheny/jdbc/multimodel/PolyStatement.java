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

package org.polypheny.jdbc.multimodel;

import lombok.Getter;
import org.polypheny.jdbc.PolyphenyConnection;
import org.polypheny.jdbc.ProtoInterfaceClient;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.StatementResponse;
import org.polypheny.jdbc.utils.CallbackQueue;

public class PolyStatement {

    private static final long SCALAR_NOT_SET = -1;
    private static final int NO_STATEMENT_ID = -1;





    @Getter
    private PolyphenyConnection connection;
    @Getter
    private int statementId;

    private void resetStatement() {
        statementId = NO_STATEMENT_ID;
    }


    private ProtoInterfaceClient getProtoInterfaceClient() {
        return connection.getProtoInterfaceClient();
    }

    private Result getResultFromFrame(Frame frame) throws ProtoInterfaceServiceException {
        switch ( frame.getResultCase() ) {
            case RELATIONAL_FRAME:
                return new RelationalResult( frame.getRelationalFrame(), this );
            case DOCUMENT_FRAME:
                return new DocumentResult( frame.getDocumentFrame(), this );
            case GRAPH_FRAME:
                return new GraphResult( frame.getGraphFrame(), this );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.RESULT_TYPE_INVALID, "Statement produced unknown result type" );
    }


    public PolyStatement( PolyphenyConnection polyphenyConnection ) {
        this.connection = polyphenyConnection;
    }


    public Result execute( String namespaceName, String languageName, String statement ) throws ProtoInterfaceServiceException {
        resetStatement();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>();
        int timeout = connection.getTimeout();
        getProtoInterfaceClient().executeUnparameterizedStatement(
                namespaceName,
                languageName,
                statement,
                callback,
                timeout
        );
        while ( true ) {
            StatementResponse response = callback.takeNext();
            if ( statementId == NO_STATEMENT_ID ) {
                statementId = response.getStatementId();
            }
            if ( !response.hasResult() ) {
                continue;
            }
            try {
                callback.awaitCompletion();
            } catch ( InterruptedException e ) {
                throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
            }
            if ( !response.getResult().hasFrame() ) {
                return new ScalarResult(response.getResult().getScalar());
            }
            return getResultFromFrame(response.getResult().getFrame());
        }
    }
}

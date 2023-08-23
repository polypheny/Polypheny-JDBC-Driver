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
import org.polypheny.jdbc.RelationalResult;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.StatementResponse;
import org.polypheny.jdbc.utils.CallbackQueue;

public class ProtoStatement {

    private static final long SCALAR_NOT_SET = -1;
    private static final int NO_STATEMENT_ID = -1;


    public enum ResultType {
        RELATIONAL,
        DOCUMENT,
        GRAPH,
        SCALAR
    }


    @Getter
    private PolyphenyConnection connection;
    @Getter
    private int statementId;
    private ResultType resultType = null;
    private RelationalResult relationalResult = null;
    private DocumentResult documentResult = null;
    private GraphResult graphResult = null;

    private long scalarResult = SCALAR_NOT_SET;


    private void resetStatement() {
        relationalResult = null;
        documentResult = null;
        graphResult = null;
        scalarResult = SCALAR_NOT_SET;
        statementId = NO_STATEMENT_ID;
    }


    private ProtoInterfaceClient getProtoInterfaceClient() {
        return connection.getProtoInterfaceClient();
    }

    private ResultType getResultFromFrame(Frame frame) throws ProtoInterfaceServiceException {
        switch ( frame.getResultCase() ) {
            case RELATIONAL_FRAME:
                relationalResult = new RelationalResult( frame.getRelationalFrame(), this );
                return ResultType.RELATIONAL;
            case DOCUMENT_FRAME:
                documentResult = new DocumentResult( frame.getDocumentFrame(), this );
                return ResultType.DOCUMENT;
            case GRAPH_FRAME:
                graphResult = new GraphResult( frame.getGraphFrame(), this );
                return ResultType.GRAPH;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.RESULT_TYPE_INVALID, "Statement produced unknown result type" );
    }


    public ProtoStatement( PolyphenyConnection polyphenyConnection ) {
        this.connection = polyphenyConnection;
    }


    public ResultType execute( String namespaceName, String languageName, String statement ) throws ProtoInterfaceServiceException {
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
                scalarResult = response.getResult().getScalar();
                return ResultType.SCALAR;
            }
            return getResultFromFrame(response.getResult().getFrame());
        }
    }


    public RelationalResult getRelationalResult() throws ProtoInterfaceServiceException {
        if ( resultType != ResultType.RELATIONAL ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.RESULT_TYPE_INVALID,
                    "This statement did not return a relational result."
            );
        }
        return relationalResult;
    }


    public DocumentResult getDocumentResult() throws ProtoInterfaceServiceException {
        if ( resultType != ResultType.DOCUMENT ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.RESULT_TYPE_INVALID,
                    "This statement did not return a document result."
            );
        }
        return documentResult;
    }


    public GraphResult getGraphResult() throws ProtoInterfaceServiceException {
        if ( resultType != ResultType.GRAPH ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.RESULT_TYPE_INVALID,
                    "This statement did not return a graph result."
            );
        }
        return graphResult;
    }


    public long getScalarResult() throws ProtoInterfaceServiceException {
        if ( resultType != ResultType.SCALAR ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.RESULT_TYPE_INVALID,
                    "This statement did not return a scalar result." );
        }
        return scalarResult;
    }

}

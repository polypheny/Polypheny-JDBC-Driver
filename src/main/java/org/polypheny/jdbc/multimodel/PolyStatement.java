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

package org.polypheny.jdbc.multimodel;

import java.util.HashMap;
import java.util.List;
import lombok.Getter;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;

public class PolyStatement {

    private static final int NO_STATEMENT_ID = -1;

    @Getter
    private PolyConnection connection;
    @Getter
    private int statementId;
    @Getter
    private boolean isPrepared;


    private void resetStatement() throws PrismInterfaceServiceException {
        if ( statementId != -1 ) {
            connection.getPrismInterfaceClient().closeStatement( statementId, connection.getTimeout() );
        }
        statementId = NO_STATEMENT_ID;
        isPrepared = false;
    }


    private PrismInterfaceClient getPrismInterfaceClient() {
        return connection.getPrismInterfaceClient();
    }


    private Result getResultFromFrame( Frame frame ) throws PrismInterfaceServiceException {
        switch ( frame.getResultCase() ) {
            case RELATIONAL_FRAME:
                return new RelationalResult( frame, this );
            case DOCUMENT_FRAME:
                return new DocumentResult( frame, this );
            case GRAPH_FRAME:
                return new GraphResult( frame, this );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement produced unknown result type" );
    }


    public PolyStatement( PolyConnection polyConnection ) {
        this.connection = polyConnection;
    }


    public Result execute( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        resetStatement();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>( Response::getStatementResponse );
        int timeout = connection.getTimeout();
        getPrismInterfaceClient().executeUnparameterizedStatement(
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
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of api call failed.", e );
            }
            if ( !response.getResult().hasFrame() ) {
                return new ScalarResult( response.getResult().getScalar() );
            }
            return getResultFromFrame( response.getResult().getFrame() );
        }
    }


    public List<Long> execute( String namespaceName, String languageName, List<String> statements ) throws PrismInterfaceServiceException, InterruptedException {
        resetStatement();
        CallbackQueue<StatementBatchResponse> callback = new CallbackQueue<>( Response::getStatementBatchResponse );
        int timeout = connection.getTimeout();
        getPrismInterfaceClient().executeUnparameterizedStatementBatch( statements, namespaceName, languageName, callback, timeout );
        while ( true ) {
            StatementBatchResponse response = callback.takeNext();
            if ( statementId == NO_STATEMENT_ID ) {
                statementId = response.getBatchId();
            }
            if ( response.getScalarsCount() == 0 ) {
                continue;
            }
            callback.awaitCompletion();
            return response.getScalarsList();
        }
    }


    public void prepare( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        int timeout = connection.getTimeout();
        if ( statement.contains( "?" ) ) {
            PreparedStatementSignature signature = getPrismInterfaceClient().prepareIndexedStatement( namespaceName, languageName, statement, timeout );
            statementId = signature.getStatementId();
            isPrepared = true;
            return;
        }
        if ( statement.contains( ":" ) ) {
            org.polypheny.prism.PreparedStatementSignature signature = connection.getPrismInterfaceClient().prepareNamedStatement( namespaceName, languageName, statement, timeout );
            statementId = signature.getStatementId();
            isPrepared = true;
            return;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Statement must be either of the indexed or named parameterized kind." );
    }


    public Result executePrepared( List<TypedValue> parameters ) throws PrismInterfaceServiceException {
        if ( !isPrepared ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "This operation requires a statmement to be prepared first" );
        }
        int timeout = connection.getTimeout();
        StatementResult result = getPrismInterfaceClient().executeIndexedStatement( statementId, parameters, PropertyUtils.getDEFAULT_FETCH_SIZE(), timeout );
        if ( !result.hasFrame() ) {
            return new ScalarResult( result.getScalar() );
        }
        return getResultFromFrame( result.getFrame() );
    }


    public Result executePrepared( HashMap<String, TypedValue> parameters ) throws PrismInterfaceServiceException {
        if ( !isPrepared ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "This operation requires a statmement to be prepared first" );
        }
        int timeout = connection.getTimeout();
        StatementResult result = getPrismInterfaceClient().executeNamedStatement( statementId, parameters, PropertyUtils.getDEFAULT_FETCH_SIZE(), timeout );
        if ( !result.hasFrame() ) {
            return new ScalarResult( result.getScalar() );
        }
        return getResultFromFrame( result.getFrame() );
    }


    public List<Long> executePreparedBatch( List<List<TypedValue>> parameterBatch ) throws PrismInterfaceServiceException {
        if ( !isPrepared ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "This operation requires a statmement to be prepared first" );
        }
        int timeout = connection.getTimeout();
        StatementBatchResponse response = getPrismInterfaceClient().executeIndexedStatementBatch( statementId, parameterBatch, timeout );
        return response.getScalarsList();
    }

}

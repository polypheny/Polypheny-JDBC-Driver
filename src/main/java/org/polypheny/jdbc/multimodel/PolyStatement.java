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

import java.util.Collections;
import java.util.List;
import java.util.Map;
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

public class PolyStatement implements AutoCloseable {

    private static final int NO_STATEMENT_ID = -1;

    @Getter
    private final PolyConnection connection;
    @Getter
    private int statementId = NO_STATEMENT_ID;
    @Getter
    private int fetchSize = PropertyUtils.getDEFAULT_FETCH_SIZE();
    private ParameterKind parameterKind;
    private long execution;
    private boolean hasOpenResult;
    private boolean closed;


    public PolyStatement( PolyConnection polyConnection ) {
        this.connection = polyConnection;
    }


    public synchronized void setFetchSize( int fetchSize ) throws PrismInterfaceServiceException {
        checkOpen();
        if ( fetchSize < 0 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Fetch size must be nonnegative." );
        }
        this.fetchSize = fetchSize == 0 ? PropertyUtils.getDEFAULT_FETCH_SIZE() : fetchSize;
    }


    public synchronized boolean isPrepared() {
        return parameterKind != null;
    }


    @Override
    public synchronized void close() throws PrismInterfaceServiceException {
        if ( !closed ) {
            resetStatement();
            closed = true;
        }
    }


    private void resetStatement() throws PrismInterfaceServiceException {
        if ( statementId != NO_STATEMENT_ID && !connection.isClosed() ) {
            getPrismInterfaceClient().closeStatement( statementId, connection.getTimeout() );
        }
        statementId = NO_STATEMENT_ID;
        parameterKind = null;
        hasOpenResult = false;
        execution++;
    }


    private void checkOpen() throws PrismInterfaceServiceException {
        if ( closed || connection.isClosed() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "The statement or its connection is closed." );
        }
    }


    private PrismInterfaceClient getPrismInterfaceClient() {
        return connection.getPrismInterfaceClient();
    }


    synchronized long getExecution() {
        return execution;
    }


    synchronized Frame fetchFrame( long resultExecution, int resultFetchSize, Frame.ResultCase expectedType ) throws PrismInterfaceServiceException {
        checkOpen();
        if ( resultExecution != execution ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "The statement has been re-executed since this result was opened." );
        }
        Frame frame = getPrismInterfaceClient().fetchResult( statementId, resultFetchSize, connection.getTimeout() );
        if ( frame.getResultCase() != expectedType ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID,
                    "Expected " + expectedType + ", but the statement returned " + frame.getResultCase() + "." );
        }
        hasOpenResult = !frame.getIsLast();
        return frame;
    }


    private Result readResult( StatementResult result ) throws PrismInterfaceServiceException {
        if ( !result.hasFrame() ) {
            return new ScalarResult( result.getScalar() );
        }
        Frame frame = result.getFrame();
        hasOpenResult = !frame.getIsLast();
        switch ( frame.getResultCase() ) {
            case RELATIONAL_FRAME:
                return new RelationalResult( frame, this );
            case DOCUMENT_FRAME:
                return new DocumentResult( frame, this );
            case GRAPH_FRAME:
                return new GraphResult( frame, this );
            default:
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement produced an unknown result type." );
        }
    }


    public synchronized Result execute( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        checkOpen();
        resetStatement();
        CallbackQueue<StatementResponse> callback = new CallbackQueue<>( Response::getStatementResponse );
        getPrismInterfaceClient().executeUnparameterizedStatement( namespaceName, languageName, statement, fetchSize, callback, connection.getTimeout() );
        while ( true ) {
            StatementResponse response = callback.takeNext();
            statementId = response.getStatementId();
            if ( response.hasResult() ) {
                try {
                    callback.awaitSuccessfulCompletion();
                } catch ( InterruptedException e ) {
                    Thread.currentThread().interrupt();
                    throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting completion of the statement failed.", e );
                }
                return readResult( response.getResult() );
            }
        }
    }


    public synchronized List<Long> execute( String namespaceName, String languageName, List<String> statements ) throws PrismInterfaceServiceException, InterruptedException {
        checkOpen();
        resetStatement();
        if ( statements.isEmpty() ) {
            return Collections.emptyList();
        }
        CallbackQueue<StatementBatchResponse> callback = new CallbackQueue<>( Response::getStatementBatchResponse );
        getPrismInterfaceClient().executeUnparameterizedStatementBatch( statements, namespaceName, languageName, callback, connection.getTimeout() );
        while ( true ) {
            StatementBatchResponse response = callback.takeNext();
            statementId = response.getBatchId();
            if ( response.getScalarsCount() > 0 ) {
                callback.awaitSuccessfulCompletion();
                return response.getScalarsList();
            }
        }
    }


    /**
     * Prepares a statement with indexed parameters. Use {@link #prepareNamed} for named parameters.
     */
    public void prepare( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        prepareIndexed( namespaceName, languageName, statement );
    }


    public synchronized void prepareIndexed( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        checkOpen();
        resetStatement();
        PreparedStatementSignature signature = getPrismInterfaceClient().prepareIndexedStatement( namespaceName, languageName, statement, connection.getTimeout() );
        statementId = signature.getStatementId();
        parameterKind = ParameterKind.INDEXED;
    }


    public synchronized void prepareNamed( String namespaceName, String languageName, String statement ) throws PrismInterfaceServiceException {
        checkOpen();
        resetStatement();
        PreparedStatementSignature signature = getPrismInterfaceClient().prepareNamedStatement( namespaceName, languageName, statement, connection.getTimeout() );
        statementId = signature.getStatementId();
        parameterKind = ParameterKind.NAMED;
    }


    public synchronized Result executePrepared( List<TypedValue> parameters ) throws PrismInterfaceServiceException {
        beginPreparedExecution( ParameterKind.INDEXED );
        return readResult( getPrismInterfaceClient().executeIndexedStatement( statementId, parameters, fetchSize, connection.getTimeout() ) );
    }


    public synchronized Result executePrepared( Map<String, TypedValue> parameters ) throws PrismInterfaceServiceException {
        beginPreparedExecution( ParameterKind.NAMED );
        return readResult( getPrismInterfaceClient().executeNamedStatement( statementId, parameters, fetchSize, connection.getTimeout() ) );
    }


    public synchronized List<Long> executePreparedBatch( List<List<TypedValue>> parameterBatch ) throws PrismInterfaceServiceException {
        beginPreparedExecution( ParameterKind.INDEXED );
        if ( parameterBatch.isEmpty() ) {
            return Collections.emptyList();
        }
        return getPrismInterfaceClient().executeIndexedStatementBatch( statementId, parameterBatch, connection.getTimeout() ).getScalarsList();
    }


    private void beginPreparedExecution( ParameterKind kind ) throws PrismInterfaceServiceException {
        checkOpen();
        if ( parameterKind != kind ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Prepare a statement with " + kind.description + " parameters first." );
        }
        if ( hasOpenResult ) {
            getPrismInterfaceClient().closeResult( statementId, connection.getTimeout() );
            hasOpenResult = false;
        }
        execution++;
    }


    private enum ParameterKind {
        INDEXED( "indexed" ),
        NAMED( "named" );

        private final String description;


        ParameterKind( String description ) {
            this.description = description;
        }
    }

}

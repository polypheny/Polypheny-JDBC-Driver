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

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.streaming.StreamingIndex;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.jdbc.utils.ProtoUtils;
import org.polypheny.jdbc.utils.VersionUtil;
import org.polypheny.prism.ClientInfoProperties;
import org.polypheny.prism.ClientInfoPropertiesRequest;
import org.polypheny.prism.CloseResultRequest;
import org.polypheny.prism.CloseStatementRequest;
import org.polypheny.prism.CommitRequest;
import org.polypheny.prism.ConnectionCheckRequest;
import org.polypheny.prism.ConnectionProperties;
import org.polypheny.prism.ConnectionPropertiesUpdateRequest;
import org.polypheny.prism.ConnectionRequest;
import org.polypheny.prism.ConnectionResponse;
import org.polypheny.prism.DbmsVersionRequest;
import org.polypheny.prism.DbmsVersionResponse;
import org.polypheny.prism.DefaultNamespaceRequest;
import org.polypheny.prism.DisconnectRequest;
import org.polypheny.prism.EntitiesRequest;
import org.polypheny.prism.Entity;
import org.polypheny.prism.ExecuteIndexedStatementBatchRequest;
import org.polypheny.prism.ExecuteIndexedStatementRequest;
import org.polypheny.prism.ExecuteNamedStatementRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.prism.ExecuteUnparameterizedStatementRequest;
import org.polypheny.prism.FetchRequest;
import org.polypheny.prism.Frame;
import org.polypheny.prism.Function;
import org.polypheny.prism.FunctionsRequest;
import org.polypheny.prism.IndexedParameters;
import org.polypheny.prism.NamedParameters;
import org.polypheny.prism.Namespace;
import org.polypheny.prism.NamespacesRequest;
import org.polypheny.prism.PrepareStatementRequest;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.Procedure;
import org.polypheny.prism.ProceduresRequest;
import org.polypheny.prism.RollbackRequest;
import org.polypheny.prism.SqlKeywordsRequest;
import org.polypheny.prism.SqlNumericFunctionsRequest;
import org.polypheny.prism.SqlStringFunctionsRequest;
import org.polypheny.prism.SqlSystemFunctionsRequest;
import org.polypheny.prism.SqlTimeDateFunctionsRequest;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResponse;
import org.polypheny.prism.StatementResult;
import org.polypheny.prism.StreamAcknowledgement;
import org.polypheny.prism.StreamFetchRequest;
import org.polypheny.prism.StreamFrame;
import org.polypheny.prism.StreamSendRequest;
import org.polypheny.prism.TableType;
import org.polypheny.prism.TableTypesRequest;
import org.polypheny.prism.Type;
import org.polypheny.prism.TypesRequest;

public class PrismInterfaceClient {

    private final Transport con;
    private final RpcService rpc;


    public PrismInterfaceClient( String host, int port, Map<String, String> parameters ) throws PrismInterfaceServiceException {
        try {
            String transport = parameters.getOrDefault( "transport", "plain" );
            if ( transport.equals( "plain" ) ) {
                con = new PlainTransport( host, port );
            } else {
                throw new PrismInterfaceServiceException( "Unknown transport " + transport );
            }
            rpc = new RpcService( con );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( e );
        }
    }


    public boolean checkConnection( int timeout ) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            rpc.checkConnection( request, timeout );
            return true;
        } catch ( PrismInterfaceServiceException e ) {
            return false;
        }
    }


    public ConnectionResponse register( PolyphenyConnectionProperties connectionProperties, int timeout ) throws PrismInterfaceServiceException {
        ConnectionRequest.Builder requestBuilder = ConnectionRequest.newBuilder();
        Optional.ofNullable( connectionProperties.getUsername() ).ifPresent( requestBuilder::setUsername );
        Optional.ofNullable( connectionProperties.getPassword() ).ifPresent( requestBuilder::setPassword );
        requestBuilder
                .setMajorApiVersion( VersionUtil.MAJOR_API_VERSION )
                .setMinorApiVersion( VersionUtil.MINOR_API_VERSION )
                //.setClientUuid( clientUUID )
                .setConnectionProperties( buildConnectionProperties( connectionProperties ) );
        ConnectionResponse connectionResponse = rpc.connect( requestBuilder.build(), timeout );
        if ( !connectionResponse.getIsCompatible() ) {
            throw new PrismInterfaceServiceException( "client version " + getClientApiVersionString()
                    + " not compatible with server version " + getServerApiVersionString( connectionResponse ) + "." );
        }
        return connectionResponse;
    }


    private ConnectionProperties buildConnectionProperties( PolyphenyConnectionProperties properties ) {
        ConnectionProperties.Builder propertiesBuilder = ConnectionProperties.newBuilder();
        Optional.ofNullable( properties.getNamespaceName() ).ifPresent( propertiesBuilder::setNamespaceName );
        return propertiesBuilder
                .setIsAutoCommit( properties.isAutoCommit() )
                .build();
    }


    public void unregister( int timeout ) throws PrismInterfaceServiceException {
        DisconnectRequest request = DisconnectRequest.newBuilder().build();
        try {
            rpc.disconnect( request, timeout );
        } finally {
            rpc.close();
        }
    }


    public void executeUnparameterizedStatement( String namespaceName, String languageName, String statement, CallbackQueue<StatementResponse> callback, int timeout ) throws PrismInterfaceServiceException {
        ExecuteUnparameterizedStatementRequest request = buildExecuteUnparameterizedStatementRequest( statement, namespaceName, languageName );
        rpc.executeUnparameterizedStatement( request, callback ); // TODO timeout
    }


    public void executeUnparameterizedStatementBatch( List<String> statements, String namespaceName, String languageName, CallbackQueue<StatementBatchResponse> updateCallback, int timeout ) throws PrismInterfaceServiceException {
        List<ExecuteUnparameterizedStatementRequest> requests = statements.stream().map( s -> buildExecuteUnparameterizedStatementRequest( s, namespaceName, languageName ) ).collect( Collectors.toList() );
        executeUnparameterizedStatementBatch( requests, updateCallback, timeout );
    }


    public ExecuteUnparameterizedStatementRequest buildExecuteUnparameterizedStatementRequest( String statement, String namespaceName, String languageName ) {
        ExecuteUnparameterizedStatementRequest.Builder requestBuilder = ExecuteUnparameterizedStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        return requestBuilder
                .setLanguageName( languageName )
                .setStatement( statement )
                .build();
    }


    public void executeUnparameterizedStatementBatch( List<ExecuteUnparameterizedStatementRequest> requests, CallbackQueue<StatementBatchResponse> updateCallback, int timeout ) throws PrismInterfaceServiceException {
        ExecuteUnparameterizedStatementBatchRequest request = ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                .addAllStatements( requests )
                .build();
        rpc.executeUnparameterizedStatementBatch( request, updateCallback ); // TODO timeout
    }


    public PreparedStatementSignature prepareIndexedStatement( String namespaceName, String languageName, String statement, int timeout ) throws PrismInterfaceServiceException {
        PrepareStatementRequest.Builder requestBuilder = PrepareStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        PrepareStatementRequest request = requestBuilder
                .setStatement( statement )
                .setLanguageName( languageName )
                .build();

        return rpc.prepareIndexedStatement( request, timeout );
    }


    public PreparedStatementSignature prepareNamedStatement( String namespaceName, String languageName, String statement, int timeout ) throws PrismInterfaceServiceException {
        PrepareStatementRequest.Builder requestBuilder = PrepareStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        PrepareStatementRequest request = requestBuilder
                .setStatement( statement )
                .setLanguageName( languageName )
                .build();
        return rpc.prepareNamedStatement( request, timeout );
    }


    public StatementResult executeIndexedStatement( int statementId, List<TypedValue> values, int fetchSize, StreamingIndex streamingIndex, int timeout ) throws PrismInterfaceServiceException {
        IndexedParameters parameters = IndexedParameters.newBuilder()
                .addAllParameters( ProtoUtils.serializeParameterList( values, streamingIndex ) )
                .build();
        ExecuteIndexedStatementRequest request = ExecuteIndexedStatementRequest.newBuilder()
                .setStatementId( statementId )
                .setParameters( parameters )
                .setFetchSize( fetchSize )
                .build();

        return rpc.executeIndexedStatement( request, timeout );
    }


    public StatementResult executeNamedStatement( int statementId, Map<String, TypedValue> values, int fetchSize, StreamingIndex streamingIndex, int timeout ) throws PrismInterfaceServiceException {
        NamedParameters parameters = NamedParameters.newBuilder()
                .putAllParameters( ProtoUtils.serializeParameterMap( values, streamingIndex ) )
                .build();
        ExecuteNamedStatementRequest request = ExecuteNamedStatementRequest.newBuilder()
                .setStatementId( statementId )
                .setParameters( parameters )
                .setFetchSize( fetchSize )
                .build();

        return rpc.executeNamedStatement( request, timeout );
    }


    public StatementBatchResponse executeIndexedStatementBatch( int statementId, List<List<TypedValue>> parameterBatch, StreamingIndex streamingIndex, int timeout ) throws PrismInterfaceServiceException {
        List<IndexedParameters> parameters = parameterBatch.stream()
                .map( l -> ProtoUtils.serializeParameterList( l, streamingIndex ) )
                .map( p -> IndexedParameters.newBuilder().addAllParameters( p ).build() )
                .collect( Collectors.toList() );
        ExecuteIndexedStatementBatchRequest request = ExecuteIndexedStatementBatchRequest.newBuilder()
                .setStatementId( statementId )
                .addAllParameters( parameters )
                .build();

        return rpc.executeIndexedStatementBatch( request, timeout );
    }


    public void commitTransaction( int timeout ) throws PrismInterfaceServiceException {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();

        rpc.commit( commitRequest, timeout );
    }


    public void rollbackTransaction( int timeout ) throws PrismInterfaceServiceException {
        RollbackRequest rollbackRequest = RollbackRequest.newBuilder().build();

        rpc.rollback( rollbackRequest, timeout );
    }


    public void closeStatement( int statementId, int timeout ) throws PrismInterfaceServiceException {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId( statementId )
                .build();

        rpc.closeStatement( request, timeout );
    }


    public void closeResult( int statementId, int timeout ) throws PrismInterfaceServiceException {
        CloseResultRequest resultCloseRequest = CloseResultRequest.newBuilder()
                .setStatementId( statementId )
                .build();

        rpc.closeResult( resultCloseRequest, timeout );
    }


    public Frame fetchResult( int statementId, int fetchSize, int timeout ) throws PrismInterfaceServiceException {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setFetchSize( fetchSize )
                .setStatementId( statementId )
                .build();

        return rpc.fetchResult( fetchRequest, timeout );
    }


    public StreamFrame fetchStream( int statementId, long streamId, long position, int length, int timeout ) throws PrismInterfaceServiceException {
        StreamFetchRequest streamFetchRequest = StreamFetchRequest.newBuilder()
                .setStatementId( statementId )
                .setStreamId( streamId )
                .setPosition( position )
                .setLength( length )
                .build();
        return rpc.fetchStream( streamFetchRequest, timeout );
    }


    public StreamAcknowledgement streamBinary( byte[] bytes, boolean is_last, long streamId, int timeout ) throws PrismInterfaceServiceException {
        StreamFrame frame = StreamFrame.newBuilder()
                .setBinary( ByteString.copyFrom( bytes ) )
                .setIsLast( is_last )
                .build();
        StreamSendRequest streamSendRequest = StreamSendRequest.newBuilder()
                .setFrame( frame )
                .setStreamId( streamId )
                .build();
        return rpc.stream( streamSendRequest, timeout );
    }


    private String getServerApiVersionString( ConnectionResponse response ) {
        return response.getMajorApiVersion() + "." + response.getMinorApiVersion();
    }


    private static String getClientApiVersionString() {
        return VersionUtil.MAJOR + "." + VersionUtil.MINOR;
    }


    public DbmsVersionResponse getDbmsVersion( int timeout ) throws PrismInterfaceServiceException {
        DbmsVersionRequest dbmsVersionRequest = DbmsVersionRequest.newBuilder().build();

        return rpc.getDbmsVersion( dbmsVersionRequest, timeout );
    }


    public String getDefaultNamespace( int timeout ) throws PrismInterfaceServiceException {
        return rpc.defaultNamespaceRequest( DefaultNamespaceRequest.newBuilder().build(), timeout ).getDefaultNamespace();
    }


    public List<Type> getTypes( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getTypes( TypesRequest.newBuilder().build(), timeout ).getTypesList();
    }


    public String getSqlStringFunctions( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getSqlStringFunctions( SqlStringFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlSystemFunctions( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getSqlSystemFunctions( SqlSystemFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlTimeDateFunctions( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlNumericFunctions( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getSqlNumericFunctions( SqlNumericFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlKeywords( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getSqlKeywords( SqlKeywordsRequest.newBuilder().build(), timeout ).getString();
    }


    public void setConnectionProperties( PolyphenyConnectionProperties connectionProperties, int timeout ) throws PrismInterfaceServiceException {
        ConnectionPropertiesUpdateRequest request = ConnectionPropertiesUpdateRequest.newBuilder()
                .setConnectionProperties( buildConnectionProperties( connectionProperties ) )
                .build();
        rpc.updateConnectionProperties( request, timeout );
    }


    public List<Procedure> searchProcedures( String languageName, String procedureNamePattern, int timeout ) throws PrismInterfaceServiceException {
        ProceduresRequest.Builder requestBuilder = ProceduresRequest.newBuilder();
        requestBuilder.setLanguage( languageName );
        Optional.ofNullable( procedureNamePattern ).ifPresent( requestBuilder::setProcedureNamePattern );
        return rpc.searchProcedures( requestBuilder.build(), timeout ).getProceduresList();
    }


    public Map<String, String> getClientInfoProperties( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getClientInfoProperties( ClientInfoPropertiesRequest.newBuilder().build(), timeout ).getPropertiesMap();
    }


    public List<Namespace> searchNamespaces( String schemaPattern, String protoNamespaceType, int timeout ) throws PrismInterfaceServiceException {
        NamespacesRequest.Builder requestBuilder = NamespacesRequest.newBuilder();
        Optional.ofNullable( schemaPattern ).ifPresent( requestBuilder::setNamespacePattern );
        Optional.ofNullable( protoNamespaceType ).ifPresent( requestBuilder::setNamespaceType );

        return rpc.searchNamespaces( requestBuilder.build(), timeout ).getNamespacesList();
    }


    public List<Entity> searchEntities( String namespace, String entityNamePattern, int timeout ) throws PrismInterfaceServiceException {
        EntitiesRequest.Builder requestBuilder = EntitiesRequest.newBuilder();
        requestBuilder.setNamespaceName( namespace );
        Optional.ofNullable( entityNamePattern ).ifPresent( requestBuilder::setEntityPattern );

        return rpc.searchEntities( requestBuilder.build(), timeout ).getEntitiesList();
    }


    public List<TableType> getTablesTypes( int timeout ) throws PrismInterfaceServiceException {
        return rpc.getTableTypes( TableTypesRequest.newBuilder().build(), timeout ).getTableTypesList();
    }


    public void setClientInfoProperties( Properties properties, int timeout ) throws PrismInterfaceServiceException {
        ClientInfoProperties.Builder requestBuilder = ClientInfoProperties.newBuilder();
        properties.stringPropertyNames().forEach( s -> requestBuilder.putProperties( s, properties.getProperty( s ) ) );
        rpc.setClientInfoProperties( requestBuilder.build(), timeout );
    }


    public List<Function> searchFunctions( String languageName, String functionCategory, int timeout ) throws PrismInterfaceServiceException {
        FunctionsRequest functionsRequest = FunctionsRequest.newBuilder()
                .setQueryLanguage( languageName )
                .setFunctionCategory( functionCategory )
                .build();

        return rpc.searchFunctions( functionsRequest, timeout ).getFunctionsList();
    }

}

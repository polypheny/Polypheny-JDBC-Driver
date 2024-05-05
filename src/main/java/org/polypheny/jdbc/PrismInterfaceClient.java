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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.ClientInfoProperties;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesRequest;
import org.polypheny.db.protointerface.proto.CloseResultRequest;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.ConnectionCheckRequest;
import org.polypheny.db.protointerface.proto.ConnectionProperties;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateRequest;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.ConnectionResponse;
import org.polypheny.db.protointerface.proto.DbmsVersionRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionResponse;
import org.polypheny.db.protointerface.proto.DefaultNamespaceRequest;
import org.polypheny.db.protointerface.proto.DisconnectRequest;
import org.polypheny.db.protointerface.proto.EntitiesRequest;
import org.polypheny.db.protointerface.proto.Entity;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteIndexedStatementRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementBatchRequest;
import org.polypheny.db.protointerface.proto.ExecuteUnparameterizedStatementRequest;
import org.polypheny.db.protointerface.proto.FetchRequest;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.Function;
import org.polypheny.db.protointerface.proto.FunctionsRequest;
import org.polypheny.db.protointerface.proto.IndexedParameters;
import org.polypheny.db.protointerface.proto.Namespace;
import org.polypheny.db.protointerface.proto.NamespacesRequest;
import org.polypheny.db.protointerface.proto.PrepareStatementRequest;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.Procedure;
import org.polypheny.db.protointerface.proto.ProceduresRequest;
import org.polypheny.db.protointerface.proto.RollbackRequest;
import org.polypheny.db.protointerface.proto.SqlKeywordsRequest;
import org.polypheny.db.protointerface.proto.SqlNumericFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlStringFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlSystemFunctionsRequest;
import org.polypheny.db.protointerface.proto.SqlTimeDateFunctionsRequest;
import org.polypheny.db.protointerface.proto.StatementBatchResponse;
import org.polypheny.db.protointerface.proto.StatementResponse;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.db.protointerface.proto.TableType;
import org.polypheny.db.protointerface.proto.TableTypesRequest;
import org.polypheny.db.protointerface.proto.Type;
import org.polypheny.db.protointerface.proto.TypesRequest;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.transport.PlainTransport;
import org.polypheny.jdbc.transport.Transport;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.jdbc.utils.ProtoUtils;
import org.polypheny.jdbc.utils.VersionUtil;

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
                .setMajorApiVersion( VersionUtil.getMajor() )
                .setMinorApiVersion( VersionUtil.getMinor() )
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
        ExecuteUnparameterizedStatementRequest.Builder requestBuilder = ExecuteUnparameterizedStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        ExecuteUnparameterizedStatementRequest request = requestBuilder
                .setLanguageName( languageName )
                .setStatement( statement )
                .build();
        rpc.executeUnparameterizedStatement( request, callback ); // TODO timeout
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


    public StatementResult executeIndexedStatement( int statementId, List<TypedValue> values, int fetchSize, int timeout ) throws PrismInterfaceServiceException {
        IndexedParameters parameters = IndexedParameters.newBuilder()
                .addAllParameters( ProtoUtils.serializeParameterList( values ) )
                .build();
        ExecuteIndexedStatementRequest request = ExecuteIndexedStatementRequest.newBuilder()
                .setStatementId( statementId )
                .setParameters( parameters )
                .setFetchSize( fetchSize )
                .build();

        return rpc.executeIndexedStatement( request, timeout );
    }


    public StatementBatchResponse executeIndexedStatementBatch( int statementId, List<List<TypedValue>> parameterBatch, int timeout ) throws PrismInterfaceServiceException {
        List<IndexedParameters> parameters = parameterBatch.stream()
                .map( ProtoUtils::serializeParameterList )
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


    private String getServerApiVersionString( ConnectionResponse response ) {
        return response.getMajorApiVersion() + "." + response.getMinorApiVersion();
    }


    private static String getClientApiVersionString() {
        return VersionUtil.getMajor() + "." + VersionUtil.getMinor();
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

package org.polypheny.jdbc;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.db.protointerface.proto.ClientInfoProperties;
import org.polypheny.db.protointerface.proto.ClientInfoPropertiesRequest;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMeta;
import org.polypheny.db.protointerface.proto.ClientInfoPropertyMetaRequest;
import org.polypheny.db.protointerface.proto.CloseStatementRequest;
import org.polypheny.db.protointerface.proto.CommitRequest;
import org.polypheny.db.protointerface.proto.ConnectionCheckRequest;
import org.polypheny.db.protointerface.proto.ConnectionProperties;
import org.polypheny.db.protointerface.proto.ConnectionPropertiesUpdateRequest;
import org.polypheny.db.protointerface.proto.ConnectionRequest;
import org.polypheny.db.protointerface.proto.ConnectionResponse;
import org.polypheny.db.protointerface.proto.Database;
import org.polypheny.db.protointerface.proto.DatabasesRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionRequest;
import org.polypheny.db.protointerface.proto.DbmsVersionResponse;
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
import org.polypheny.db.protointerface.proto.LanguageRequest;
import org.polypheny.db.protointerface.proto.Namespace;
import org.polypheny.db.protointerface.proto.NamespaceRequest;
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
import org.polypheny.db.protointerface.proto.UserDefinedType;
import org.polypheny.db.protointerface.proto.UserDefinedTypesRequest;
import org.polypheny.jdbc.serialisation.ProtoValueSerializer;
import org.polypheny.jdbc.jdbctypes.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
    private final Transport con;
    private final RpcService rpc;


    public ProtoInterfaceClient( String host, int port, Map<String, String> parameters ) throws ProtoInterfaceServiceException {
        try {
            String mode = parameters.getOrDefault( "mode", "plain" );
            if ( mode.equals( "plain" ) ) {
                con = new PlainTransport( host, port );
            } else {
                throw new ProtoInterfaceServiceException( "Unknown mode " + mode );
            }
            rpc = new RpcService( con );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( e );
        }
    }


    public boolean checkConnection( int timeout ) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            // getBlockingStub( timeout ).checkConnection( request );
            rpc.checkConnection( request, timeout );
            return true;
        } catch ( ProtoInterfaceServiceException e ) {
            return false;
        }
    }


    public List<String> requestSupportedLanguages( int timeout ) {
        LanguageRequest languageRequest = LanguageRequest.newBuilder().build();
        //return blockingStub.getSupportedLanguages( languageRequest ).getLanguageNamesList();
        throw new RuntimeException( "Not yet implemented" );
    }


    public ConnectionResponse register( PolyphenyConnectionProperties connectionProperties, int timeout ) throws ProtoInterfaceServiceException {
        ConnectionRequest.Builder requestBuilder = ConnectionRequest.newBuilder();
        Optional.ofNullable( connectionProperties.getUsername() ).ifPresent( requestBuilder::setUsername );
        Optional.ofNullable( connectionProperties.getPassword() ).ifPresent( requestBuilder::setPassword );
        requestBuilder
                .setMajorApiVersion( MAJOR_API_VERSION )
                .setMinorApiVersion( MINOR_API_VERSION )
                //.setClientUuid( clientUUID )
                .setConnectionProperties( buildConnectionProperties( connectionProperties ) );
        ConnectionResponse connectionResponse = rpc.connect( requestBuilder.build(), timeout );
        if ( !connectionResponse.getIsCompatible() ) {
            throw new ProtoInterfaceServiceException( "client version " + getClientApiVersionString()
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


    public void unregister( int timeout ) throws ProtoInterfaceServiceException {
        DisconnectRequest request = DisconnectRequest.newBuilder().build();
        try {
            rpc.disconnect( request, timeout );
            //getBlockingStub( timeout ).disconnect( request );
        } finally {
            rpc.close();
            con.close();
        }
    }


    public void executeUnparameterizedStatement( String namespaceName, String languageName, String statement, CallbackQueue<StatementResponse> callback, int timeout ) throws ProtoInterfaceServiceException {
        ExecuteUnparameterizedStatementRequest.Builder requestBuilder = ExecuteUnparameterizedStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        ExecuteUnparameterizedStatementRequest request = requestBuilder
                .setLanguageName( languageName )
                .setStatement( statement )
                .build();
        rpc.executeUnparameterizedStatement( request, callback ); // TODO timeout
        //getAsyncStub( timeout ).executeUnparameterizedStatement( request, callback );
    }


    public void executeUnparameterizedStatementBatch( List<ExecuteUnparameterizedStatementRequest> requests, CallbackQueue<StatementBatchResponse> updateCallback, int timeout ) throws ProtoInterfaceServiceException {
        ExecuteUnparameterizedStatementBatchRequest request = ExecuteUnparameterizedStatementBatchRequest.newBuilder()
                .addAllStatements( requests )
                .build();

        rpc.executeUnparameterizedStatementBatch( request, updateCallback ); // TODO timeout
        //getAsyncStub( timeout ).executeUnparameterizedStatementBatch( request, updateCallback );
    }


    public PreparedStatementSignature prepareIndexedStatement( String namespaceName, String languageName, String statement, int timeout ) throws ProtoInterfaceServiceException {
        PrepareStatementRequest.Builder requestBuilder = PrepareStatementRequest.newBuilder();
        if ( namespaceName != null ) {
            requestBuilder.setNamespaceName( namespaceName );
        }
        PrepareStatementRequest request = requestBuilder
                .setStatement( statement )
                .setLanguageName( languageName )
                .build();

        //return getBlockingStub( timeout ).prepareIndexedStatement( request );
        return rpc.prepareIndexedStatement( request, timeout );
    }


    public StatementResult executeIndexedStatement( int statementId, List<TypedValue> values, int fetchSize, int timeout ) throws ProtoInterfaceServiceException {
        IndexedParameters parameters = IndexedParameters.newBuilder()
                .addAllParameters( ProtoValueSerializer.serializeParameterList( values ) )
                .build();
        ExecuteIndexedStatementRequest request = ExecuteIndexedStatementRequest.newBuilder()
                .setStatementId( statementId )
                .setParameters( parameters )
                .setFetchSize( fetchSize )
                .build();

        //return getBlockingStub( timeout ).executeIndexedStatement( request );
        return rpc.executeIndexedStatement( request, timeout );
    }


    public StatementBatchResponse executeIndexedStatementBatch( int statementId, List<List<TypedValue>> parameterBatch, int timeout ) throws ProtoInterfaceServiceException {
        List<IndexedParameters> parameters = parameterBatch.stream()
                .map( ProtoValueSerializer::serializeParameterList )
                .map( p -> IndexedParameters.newBuilder().addAllParameters( p ).build() )
                .collect( Collectors.toList() );
        ExecuteIndexedStatementBatchRequest request = ExecuteIndexedStatementBatchRequest.newBuilder()
                .setStatementId( statementId )
                .addAllParameters( parameters )
                .build();

        //return getBlockingStub( timeout ).executeIndexedStatementBatch( request );
        return rpc.executeIndexedStatementBatch( request, timeout );
    }


    public void commitTransaction( int timeout ) throws ProtoInterfaceServiceException {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();

        //getBlockingStub( timeout ).commitTransaction( commitRequest );
        rpc.commit( commitRequest, timeout );
    }


    public void rollbackTransaction( int timeout ) throws ProtoInterfaceServiceException {
        RollbackRequest rollbackRequest = RollbackRequest.newBuilder().build();

        //getBlockingStub( timeout ).rollbackTransaction( rollbackRequest );
        rpc.rollback( rollbackRequest, timeout );
    }


    public void closeStatement( int statementId, int timeout ) throws ProtoInterfaceServiceException {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId( statementId )
                .build();

        //getBlockingStub( timeout ).closeStatement( request );
        rpc.closeStatement( request, timeout );
    }


    public Frame fetchResult( int statementId, int fetchSize, int timeout ) throws ProtoInterfaceServiceException {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setFetchSize( fetchSize )
                .setStatementId( statementId )
                .build();

        //return getBlockingStub( timeout ).fetchResult( fetchRequest );
        return rpc.fetchResult( fetchRequest, timeout );
    }


    private String getServerApiVersionString( ConnectionResponse repsonse ) {
        return repsonse.getMajorApiVersion() + "." + repsonse.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


    public DbmsVersionResponse getDbmsVersion( int timeout ) throws ProtoInterfaceServiceException {
        DbmsVersionRequest dbmsVersionRequest = DbmsVersionRequest.newBuilder().build();

        //return getBlockingStub( timeout ).getDbmsVersion( dbmsVersionRequest );
        return rpc.getDbmsVersion( dbmsVersionRequest, timeout );
    }


    public List<Database> getDatabases( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getDatabases( DatabasesRequest.newBuilder().build() ).getDatabasesList();
        return rpc.getDatabases( DatabasesRequest.newBuilder().build(), timeout ).getDatabasesList();
    }


    public List<ClientInfoPropertyMeta> getClientInfoPropertyMetas( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getClientInfoPropertyMetas( ClientInfoPropertyMetaRequest.newBuilder().build() ).getClientInfoPropertyMetasList();
        return rpc.getClientInfoPropertiesMetas( ClientInfoPropertyMetaRequest.newBuilder().build(), timeout ).getClientInfoPropertyMetasList();
    }


    public List<Type> getTypes( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getTypes( TypesRequest.newBuilder().build() ).getTypesList();
        return rpc.getTypes( TypesRequest.newBuilder().build(), timeout ).getTypesList();
    }


    public String getSqlStringFunctions( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getSqlStringFunctions( SqlStringFunctionsRequest.newBuilder().build() ).getString();
        return rpc.getSqlStringFunctions( SqlStringFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlSystemFunctions( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getSqlSystemFunctions( SqlSystemFunctionsRequest.newBuilder().build() ).getString();
        return rpc.getSqlSystemFunctions( SqlSystemFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlTimeDateFunctions( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest.newBuilder().build() ).getString();
        return rpc.getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlNumericFunctions( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getSqlNumericFunctions( SqlNumericFunctionsRequest.newBuilder().build() ).getString();
        return rpc.getSqlNumericFunctions( SqlNumericFunctionsRequest.newBuilder().build(), timeout ).getString();
    }


    public String getSqlKeywords( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getSqlKeywords( SqlKeywordsRequest.newBuilder().build() ).getString();
        return rpc.getSqlKeywords( SqlKeywordsRequest.newBuilder().build(), timeout ).getString();
    }


    public void setConnectionProperties( PolyphenyConnectionProperties connectionProperties, int timeout ) throws ProtoInterfaceServiceException {
        ConnectionPropertiesUpdateRequest request = ConnectionPropertiesUpdateRequest.newBuilder()
                .setConnectionProperties( buildConnectionProperties( connectionProperties ) )
                .build();
        //getBlockingStub( timeout ).updateConnectionProperties( request );
        rpc.updateConnectionProperties( request, timeout );
    }


    public List<Procedure> searchProcedures( String languageName, String procedureNamePattern, int timeout ) throws ProtoInterfaceServiceException {
        ProceduresRequest.Builder requestBuilder = ProceduresRequest.newBuilder();
        requestBuilder.setLanguage( languageName );
        Optional.ofNullable( procedureNamePattern ).ifPresent( requestBuilder::setProcedureNamePattern );
        //return getBlockingStub( timeout ).searchProcedures( requestBuilder.build() ).getProceduresList();
        return rpc.searchProcedures( requestBuilder.build(), timeout ).getProceduresList();
    }


    public Map<String, String> getClientInfoProperties( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getClientInfoProperties( ClientInfoPropertiesRequest.newBuilder().build() ).getPropertiesMap();
        return rpc.getClientInfoProperties( ClientInfoPropertiesRequest.newBuilder().build(), timeout ).getPropertiesMap();
    }


    public List<Namespace> searchNamespaces( String schemaPattern, String protoNamespaceType, int timeout ) throws ProtoInterfaceServiceException {
        NamespacesRequest.Builder requestBuilder = NamespacesRequest.newBuilder();
        Optional.ofNullable( schemaPattern ).ifPresent( requestBuilder::setNamespacePattern );
        Optional.ofNullable( protoNamespaceType ).ifPresent( requestBuilder::setNamespaceType );

        //return getBlockingStub( timeout ).searchNamespaces( requestBuilder.build() ).getNamespacesList();
        return rpc.searchNamespaces( requestBuilder.build(), timeout ).getNamespacesList();
    }


    public List<Entity> searchEntities( String namespace, String entityNamePattern, int timeout ) throws ProtoInterfaceServiceException {
        EntitiesRequest.Builder requestBuilder = EntitiesRequest.newBuilder();
        requestBuilder.setNamespaceName( namespace );
        Optional.ofNullable( entityNamePattern ).ifPresent( requestBuilder::setEntityPattern );

        //return getBlockingStub( timeout ).searchEntities( requestBuilder.build() ).getEntitiesList();
        return rpc.searchEntities( requestBuilder.build(), timeout ).getEntitiesList();
    }


    public List<TableType> getTablesTypes( int timeout ) throws ProtoInterfaceServiceException {
        //return getBlockingStub( timeout ).getTableTypes( TableTypesRequest.newBuilder().build() ).getTableTypesList();
        return rpc.getTableTypes( TableTypesRequest.newBuilder().build(), timeout ).getTableTypesList();
    }


    public Namespace getNamespace( String namespaceName, int timeout ) throws ProtoInterfaceServiceException {
        NamespaceRequest.Builder requestBuilder = NamespaceRequest.newBuilder();
        requestBuilder.setNamespaceName( namespaceName );

        //return getBlockingStub( timeout ).getNamespace( requestBuilder.build() );
        throw new RuntimeException( "Not yet implemented" );
    }


    public List<UserDefinedType> getUserDefinedTypes( int timeout ) throws ProtoInterfaceServiceException {
        UserDefinedTypesRequest.Builder requestBuilder = UserDefinedTypesRequest.newBuilder();

        //return getBlockingStub( timeout ).getUserDefinedTypes( requestBuilder.build() ).getUserDefinedTypesList();
        return rpc.getUserDefinedTypes( requestBuilder.build(), timeout ).getUserDefinedTypesList();
    }


    public void setClientInfoProperties( Properties properties, int timeout ) throws ProtoInterfaceServiceException {
        ClientInfoProperties.Builder requestBuilder = ClientInfoProperties.newBuilder();
        properties.stringPropertyNames()
                .forEach( s -> requestBuilder.putProperties( s, properties.getProperty( s ) ) );

        //getBlockingStub( timeout ).setClientInfoProperties( requestBuilder.build() );
        rpc.setClientInfoProperties( requestBuilder.build(), timeout );
    }


    public List<Function> searchFunctions( String languaheName, String functionCategory, int timeout ) throws ProtoInterfaceServiceException {
        FunctionsRequest functionsRequest = FunctionsRequest.newBuilder()
                .setQueryLanguage( languaheName )
                .setFunctionCategory( functionCategory )
                .build();

        //return getBlockingStub( timeout ).searchFunctions( functionsRequest ).getFunctionsList();
        return rpc.searchFunctions( functionsRequest, timeout ).getFunctionsList();
    }

}
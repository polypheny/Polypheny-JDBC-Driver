package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.proto.ClientInfoProperties;
import org.polypheny.jdbc.proto.ClientInfoPropertiesRequest;
import org.polypheny.jdbc.proto.ClientInfoPropertyMeta;
import org.polypheny.jdbc.proto.ClientInfoPropertyMetaRequest;
import org.polypheny.jdbc.proto.CloseStatementRequest;
import org.polypheny.jdbc.proto.CommitRequest;
import org.polypheny.jdbc.proto.ConnectionCheckRequest;
import org.polypheny.jdbc.proto.ConnectionProperties;
import org.polypheny.jdbc.proto.ConnectionReply;
import org.polypheny.jdbc.proto.ConnectionRequest;
import org.polypheny.jdbc.proto.Database;
import org.polypheny.jdbc.proto.DatabasesRequest;
import org.polypheny.jdbc.proto.DbmsVersionRequest;
import org.polypheny.jdbc.proto.DbmsVersionResponse;
import org.polypheny.jdbc.proto.DisconnectionRequest;
import org.polypheny.jdbc.proto.EntitiesRequest;
import org.polypheny.jdbc.proto.Entity;
import org.polypheny.jdbc.proto.FetchRequest;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Function;
import org.polypheny.jdbc.proto.FunctionsRequest;
import org.polypheny.jdbc.proto.IndexedParameterBatch;
import org.polypheny.jdbc.proto.LanguageRequest;
import org.polypheny.jdbc.proto.Namespace;
import org.polypheny.jdbc.proto.NamespaceRequest;
import org.polypheny.jdbc.proto.NamespacesRequest;
import org.polypheny.jdbc.proto.ParameterList;
import org.polypheny.jdbc.proto.PreparedStatement;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.proto.Procedure;
import org.polypheny.jdbc.proto.ProceduresRequest;
import org.polypheny.jdbc.proto.ProtoInterfaceGrpc;
import org.polypheny.jdbc.proto.RollbackRequest;
import org.polypheny.jdbc.proto.SqlKeywordsRequest;
import org.polypheny.jdbc.proto.SqlNumericFunctionsRequest;
import org.polypheny.jdbc.proto.SqlStringFunctionsRequest;
import org.polypheny.jdbc.proto.SqlSystemFunctionsRequest;
import org.polypheny.jdbc.proto.SqlTimeDateFunctionsRequest;
import org.polypheny.jdbc.proto.StatementBatchStatus;
import org.polypheny.jdbc.proto.StatementProperties;
import org.polypheny.jdbc.proto.StatementResult;
import org.polypheny.jdbc.proto.StatementStatus;
import org.polypheny.jdbc.proto.TableType;
import org.polypheny.jdbc.proto.TableTypesRequest;
import org.polypheny.jdbc.proto.Type;
import org.polypheny.jdbc.proto.TypesRequest;
import org.polypheny.jdbc.proto.UnparameterizedStatement;
import org.polypheny.jdbc.proto.UnparameterizedStatementBatch;
import org.polypheny.jdbc.proto.UserDefinedType;
import org.polypheny.jdbc.proto.UserDefinedTypesRequest;
import org.polypheny.jdbc.serialisation.ProtoValueSerializer;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
    private static final String SQL_LANGUAGE_NAME = "sql";
    private final ProtoInterfaceGrpc.ProtoInterfaceBlockingStub blockingStub;
    private final ProtoInterfaceGrpc.ProtoInterfaceStub asyncStub;
    private final String clientUUID;


    public ProtoInterfaceClient( String target ) throws ProtoInterfaceServiceException {
        this.clientUUID = UUID.randomUUID().toString();
        try {
            Channel channel = Grpc.newChannelBuilder( target, InsecureChannelCredentials.create() )
                    .intercept( new ClientMetaInterceptor( clientUUID ) )
                    .build();
            this.blockingStub = ProtoInterfaceGrpc.newBlockingStub( channel );
            this.asyncStub = ProtoInterfaceGrpc.newStub( channel );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private ProtoInterfaceGrpc.ProtoInterfaceBlockingStub getBlockingStub( int timeout ) throws ProtoInterfaceServiceException {
        if ( timeout == 0 ) {
            return blockingStub;
        }
        try {
            return blockingStub.withDeadlineAfter( timeout, TimeUnit.SECONDS );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private ProtoInterfaceGrpc.ProtoInterfaceStub getAsyncStub( int timeout ) throws ProtoInterfaceServiceException {
        if ( timeout == 0 ) {
            return asyncStub;
        }
        try {
            return asyncStub.withDeadlineAfter( timeout, TimeUnit.SECONDS );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( e.getMessage(), Status.trailersFromThrowable( e ) );
        }
    }


    public boolean checkConnection( int timeout ) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            blockingStub.withDeadlineAfter( timeout, TimeUnit.MILLISECONDS ).checkConnection( request );
        } catch ( StatusRuntimeException e ) {
            return false;
        }
        return true;
    }


    public List<String> requestSupportedLanguages( int timeout ) throws ProtoInterfaceServiceException {
        LanguageRequest languageRequest = LanguageRequest.newBuilder().build();
        try {
            return blockingStub.getSupportedLanguages( languageRequest ).getLanguageNamesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public ConnectionReply register( PolyphenyConnectionProperties connectionProperties, int timeout ) throws ProtoInterfaceServiceException {
        try {
            ConnectionRequest.Builder requestBuilder = ConnectionRequest.newBuilder();
            Optional.ofNullable( connectionProperties.getUsername() ).ifPresent( requestBuilder::setUsername );
            Optional.ofNullable( connectionProperties.getPassword() ).ifPresent( requestBuilder::setPassword );
            requestBuilder
                    .setMajorApiVersion( MAJOR_API_VERSION )
                    .setMinorApiVersion( MINOR_API_VERSION )
                    .setClientUuid( clientUUID )
                    .setConnectionProperties( buildConnectionProperties( connectionProperties ) );
            ConnectionReply connectionReply = getBlockingStub( timeout ).connect( requestBuilder.build() );
            if ( !connectionReply.getIsCompatible() ) {
                throw new ProtoInterfaceServiceException( "client version " + getClientApiVersionString()
                        + "not compatible with server version " + getServerApiVersionString( connectionReply ) + "." );
            }
            return connectionReply;
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void unregister( int timeout ) throws ProtoInterfaceServiceException {
        DisconnectionRequest request = DisconnectionRequest.newBuilder().build();
        try {
            getBlockingStub( timeout ).disconnect( request );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private ConnectionProperties buildConnectionProperties( PolyphenyConnectionProperties polyphenyConnectionProperties ) {
        ConnectionProperties.Builder propertiesBuilder = ConnectionProperties.newBuilder();
        Optional.ofNullable( polyphenyConnectionProperties.getNamespaceName() ).ifPresent( propertiesBuilder::setNamespaceName );
        return propertiesBuilder
                .setIsAutoCommit( polyphenyConnectionProperties.isAutoCommit() )
                .setIsReadOnly( polyphenyConnectionProperties.isReadOnly() )
                .setIsolation( PropertyUtils.getProtoIsolation( polyphenyConnectionProperties.getTransactionIsolation() ) )
                .setNetworkTimeout( polyphenyConnectionProperties.getNetworkTimeout() )
                .build();
    }


    private StatementProperties buildStatementProperties( PolyphenyStatementProperties polyphenyStatementProperties, int statementId ) {
        return StatementProperties.newBuilder()
                .setStatementId( statementId )
                .setUpdateBehaviour( PropertyUtils.getProtoUpdateBehaviour( polyphenyStatementProperties.getResultSetConcurrency() ) )
                .setFetchSize( polyphenyStatementProperties.getFetchSize() )
                .setReverseFetch( PropertyUtils.isForwardFetching( polyphenyStatementProperties.getFetchDirection() ) )
                .setMaxTotalFetchSize( polyphenyStatementProperties.getLargeMaxRows() )
                .setDoesEscapeProcessing( polyphenyStatementProperties.isDoesEscapeProcessing() )
                .setIsPoolable( polyphenyStatementProperties.isPoolable() )
                .build();
    }


    public void executeUnparameterizedStatement( PolyphenyStatementProperties properties, String statement, CallbackQueue<StatementStatus> updateCallback, int timeout ) throws ProtoInterfaceServiceException {
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub( properties.getQueryTimeoutSeconds() );
        try {
            getAsyncStub( timeout ).executeUnparameterizedStatement( buildUnparameterizedStatement( properties, statement ), updateCallback );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void executeUnparameterizedStatementBatch( PolyphenyStatementProperties properties, List<String> statements, CallbackQueue<StatementBatchStatus> updateCallback, int timeout ) throws ProtoInterfaceServiceException {
        List<UnparameterizedStatement> batch = statements.
                stream()
                .map( s -> buildUnparameterizedStatement( properties, s ) )
                .collect( Collectors.toList() );
        UnparameterizedStatementBatch unparameterizedStatementBatch = UnparameterizedStatementBatch.newBuilder()
                .addAllStatements( batch )
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub( properties.getQueryTimeoutSeconds() );
        try {
            getAsyncStub( timeout ).executeUnparameterizedStatementBatch( unparameterizedStatementBatch, updateCallback );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private UnparameterizedStatement buildUnparameterizedStatement( PolyphenyStatementProperties properties, String statement ) {
        return UnparameterizedStatement.newBuilder()
                .setStatement( statement )
                .setStatementLanguageName( SQL_LANGUAGE_NAME )
                .setProperties( (buildStatementProperties( properties, PolyphenyStatement.NO_STATEMENT_ID )) )
                .build();
    }


    public PreparedStatementSignature prepareIndexedStatement( String statement, int timeout ) throws ProtoInterfaceServiceException {
        PreparedStatement preparedStatement = PreparedStatement.newBuilder()
                .setStatement( statement )
                .setStatementLanguageName( SQL_LANGUAGE_NAME )
                .build();
        try {
            return getBlockingStub( timeout ).prepareIndexedStatement( preparedStatement );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public StatementResult executeIndexedStatement( int statementId, List<TypedValue> values, int timeout ) throws ProtoInterfaceServiceException {
        ParameterList parameterList = buildParameterList( values, statementId );
        try {
            return getBlockingStub( timeout ).executeIndexedStatement( parameterList );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public StatementBatchStatus executeIndexedStatementBatch( int statementId, List<List<TypedValue>> parameterBatch, int timeout ) throws ProtoInterfaceServiceException {
        List<ParameterList> parameterLists = parameterBatch.
                stream()
                .map( p -> buildParameterList( p, statementId ) )
                .collect( Collectors.toList() );
        IndexedParameterBatch indexedParameterBatch = IndexedParameterBatch.newBuilder()
                .setStatementId( statementId )
                .addAllParameterLists( parameterLists )
                .build();
        try {
            return getBlockingStub( timeout ).executeIndexedStatementBatch( indexedParameterBatch );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private ParameterList buildParameterList( List<TypedValue> values, int statementId ) {
        return ParameterList.newBuilder()
                .setStatementId( statementId )
                .addAllParameters( ProtoValueSerializer.serializeParameterList( values ) )
                .build();
    }


    public void commitTransaction( int timeout ) throws ProtoInterfaceServiceException {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();
        try {
            getBlockingStub( timeout ).commitTransaction( commitRequest );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void rollbackTransaction( int timeout ) throws ProtoInterfaceServiceException {
        RollbackRequest rollbackRequest = RollbackRequest.newBuilder().build();
        try {
            getBlockingStub( timeout ).rollbackTransaction( rollbackRequest );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void closeStatement( int statementId, int timeout ) throws ProtoInterfaceServiceException {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId( statementId )
                .build();
        try {
            getBlockingStub( timeout ).closeStatement( request );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public Frame fetchResult( int statementId, int timeout ) throws ProtoInterfaceServiceException {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setStatementId( statementId )
                .build();
        try {
            return getBlockingStub( timeout ).fetchResult( fetchRequest );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    private String getServerApiVersionString( ConnectionReply connectionReply ) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


    public DbmsVersionResponse getDbmsVersion( int timeout ) throws ProtoInterfaceServiceException {
        DbmsVersionRequest dbmsVersionRequest = DbmsVersionRequest.newBuilder().build();
        try {
            return getBlockingStub( timeout ).getDbmsVersion( dbmsVersionRequest );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Database> getDatabases( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getDatabases( DatabasesRequest.newBuilder().build() ).getDatabasesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<ClientInfoPropertyMeta> getClientInfoPropertyMetas( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getClientInfoPropertyMetas( ClientInfoPropertyMetaRequest.newBuilder().build() ).getClientInfoPropertyMetasList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Type> getTypes( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getTypes( TypesRequest.newBuilder().build() ).getTypesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public String getSqlStringFunctions( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getSqlStringFunctions( SqlStringFunctionsRequest.newBuilder().build() ).getString();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public String getSqlSystemFunctions( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getSqlSystemFunctions( SqlSystemFunctionsRequest.newBuilder().build() ).getString();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public String getSqlTimeDateFunctions( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getSqlTimeDateFunctions( SqlTimeDateFunctionsRequest.newBuilder().build() ).getString();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public String getSqlNumericFunctions( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getSqlNumericFunctions( SqlNumericFunctionsRequest.newBuilder().build() ).getString();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public String getSqlKeywords( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getSqlKeywords( SqlKeywordsRequest.newBuilder().build() ).getString();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void setConnectionProperties( PolyphenyConnectionProperties connectionProperties, int timeout ) throws ProtoInterfaceServiceException {
        try {
            getBlockingStub( timeout ).updateConnectionProperties( buildConnectionProperties( connectionProperties ) );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void setStatementProperties( PolyphenyStatementProperties statementProperties, int statementId, int timeout ) throws ProtoInterfaceServiceException {
        try {
            getBlockingStub( timeout ).updateStatementProperties( buildStatementProperties( statementProperties, statementId ) );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Procedure> searchProcedures( String languageName, String procedureNamePattern, int timeout ) throws ProtoInterfaceServiceException {
        ProceduresRequest.Builder requestBuilder = ProceduresRequest.newBuilder();
        requestBuilder.setLanguage( languageName );
        Optional.ofNullable( procedureNamePattern ).ifPresent( requestBuilder::setProcedureNamePattern );
        try {
            return getBlockingStub( timeout ).searchProcedures( requestBuilder.build() ).getProceduresList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public Map<String, String> getClientInfoProperties( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getClientInfoProperties( ClientInfoPropertiesRequest.newBuilder().build() ).getPropertiesMap();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Namespace> searchNamespaces( String schemaPattern, String protoNamespaceType, int timeout ) throws ProtoInterfaceServiceException {
        NamespacesRequest.Builder requestBuilder = NamespacesRequest.newBuilder();
        Optional.ofNullable( schemaPattern ).ifPresent( requestBuilder::setNamespacePattern );
        Optional.ofNullable( protoNamespaceType ).ifPresent( requestBuilder::setNamespaceType );
        try {
            return getBlockingStub( timeout ).searchNamespaces( requestBuilder.build() ).getNamespacesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Entity> searchEntities( String namespace, String entityNamePattern, int timeout ) throws ProtoInterfaceServiceException {
        EntitiesRequest.Builder requestBuilder = EntitiesRequest.newBuilder();
        requestBuilder.setNamespaceName( namespace );
        Optional.ofNullable( entityNamePattern ).ifPresent( requestBuilder::setEntityPattern );
        try {
            return getBlockingStub( timeout ).searchEntities( requestBuilder.build() ).getEntitiesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<TableType> getTablesTypes( int timeout ) throws ProtoInterfaceServiceException {
        try {
            return getBlockingStub( timeout ).getTableTypes( TableTypesRequest.newBuilder().build() ).getTableTypesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public Namespace getNamespace( String namespaceName, int timeout ) throws ProtoInterfaceServiceException {
        NamespaceRequest.Builder requestBuilder = NamespaceRequest.newBuilder();
        requestBuilder.setNamespaceName( namespaceName );
        try {
            return getBlockingStub( timeout ).getNamespace( requestBuilder.build() );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<UserDefinedType> getUserDefinedTypes( int timeout ) throws ProtoInterfaceServiceException {
        UserDefinedTypesRequest.Builder requestBuilder = UserDefinedTypesRequest.newBuilder();
        try {
            return getBlockingStub( timeout ).getUserDefinedTypes( requestBuilder.build() ).getUserDefinedTypesList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public void setClientInfoProperties( Properties properties, int timeout ) throws ProtoInterfaceServiceException {
        ClientInfoProperties.Builder requestBuilder = ClientInfoProperties.newBuilder();
        properties.stringPropertyNames()
                .forEach( s -> requestBuilder.putProperties( s, properties.getProperty( s ) ) );
        try {
            getBlockingStub( timeout ).setClientInfoProperties( requestBuilder.build() );
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }


    public List<Function> searchFunctions( String languaheName, String functionCategory, int timeout ) throws ProtoInterfaceServiceException {
        FunctionsRequest functionsRequest = FunctionsRequest.newBuilder()
                .setQueryLanguage( languaheName )
                .setFunctionCategory( functionCategory )
                .build();
        try {
            return getBlockingStub( timeout ).searchFunctions( functionsRequest ).getFunctionsList();
        } catch ( StatusRuntimeException e ) {
            throw ProtoInterfaceServiceException.fromMetadata( Status.trailersFromThrowable( e ) );
        }
    }

}
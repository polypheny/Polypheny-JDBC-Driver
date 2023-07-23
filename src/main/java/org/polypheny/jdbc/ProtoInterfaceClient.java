package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.properties.PropertyUtils;
import org.polypheny.jdbc.proto.*;
import org.polypheny.jdbc.serialisation.ProtoValueSerializer;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
    private static final String SQL_LANGUAGE_NAME = "sql";
    private final ProtoInterfaceGrpc.ProtoInterfaceBlockingStub blockingStub;
    private final ProtoInterfaceGrpc.ProtoInterfaceStub asyncStub;
    private final String clientUUID;


    public ProtoInterfaceClient(String target) throws ProtoInterfaceServiceException {
        this.clientUUID = UUID.randomUUID().toString();
        try {
            Channel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                    .intercept(new ClientMetaInterceptor(clientUUID))
                    .build();
            this.blockingStub = ProtoInterfaceGrpc.newBlockingStub(channel);
            this.asyncStub = ProtoInterfaceGrpc.newStub(channel);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException("Connection failed: " + e.getMessage());
        }
    }


    private ProtoInterfaceGrpc.ProtoInterfaceBlockingStub getBlockingStub(int timeout) {
        if (timeout == 0) {
            return blockingStub;
        }
        try {
            return blockingStub.withDeadlineAfter(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    private ProtoInterfaceGrpc.ProtoInterfaceStub getAsyncStub(int timeout) {
        if (timeout == 0) {
            return asyncStub;
        }
        try {
            return asyncStub.withDeadlineAfter(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public boolean checkConnection(int timeout) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            blockingStub.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).checkConnection(request);
        } catch (Exception e) {
            return false;
        }
        return true;
    }


    public List<String> requestSupportedLanguages() {
        LanguageRequest languageRequest = LanguageRequest.newBuilder().build();
        try {
            return blockingStub.getSupportedLanguages(languageRequest).getLanguageNamesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public ConnectionReply register(PolyphenyConnectionProperties connectionProperties) throws ProtoInterfaceServiceException {
        try {
            ConnectionRequest.Builder requestBuilder = ConnectionRequest.newBuilder();
            Optional.ofNullable(connectionProperties.getUsername()).ifPresent(requestBuilder::setUsername);
            Optional.ofNullable(connectionProperties.getPassword()).ifPresent(requestBuilder::setPassword);
            requestBuilder
                    .setMajorApiVersion(MAJOR_API_VERSION)
                    .setMinorApiVersion(MINOR_API_VERSION)
                    .setClientUuid(clientUUID)
                    .setConnectionProperties(buildConnectionProperties(connectionProperties));
            ConnectionReply connectionReply = blockingStub.connect(requestBuilder.build());
            if (!connectionReply.getIsCompatible()) {
                throw new ProtoInterfaceServiceException("client version " + getClientApiVersionString()
                        + "not compatible with server version " + getServerApiVersionString(connectionReply) + ".");
            }
            return connectionReply;
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public void unregister() {
        DisconnectionRequest request = DisconnectionRequest.newBuilder().build();
        try {
            blockingStub.disconnect(request);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    private ConnectionProperties buildConnectionProperties(PolyphenyConnectionProperties polyphenyConnectionProperties) {
        ConnectionProperties.Builder propertiesBuilder = ConnectionProperties.newBuilder();
        Optional.ofNullable(polyphenyConnectionProperties.getNamespaceName()).ifPresent(propertiesBuilder::setNamespaceName);
        return propertiesBuilder
                .setIsAutoCommit(polyphenyConnectionProperties.isAutoCommit())
                .setIsReadOnly(polyphenyConnectionProperties.isReadOnly())
                .setIsolation(PropertyUtils.getProtoIsolation(polyphenyConnectionProperties.getTransactionIsolation()))
                .setNetworkTimeout(polyphenyConnectionProperties.getNetworkTimeout())
                .build();
    }

    private StatementProperties buildStatementProperties(PolyphenyStatementProperties polyphenyStatementProperties, int statementId) {
        return StatementProperties.newBuilder()
                .setStatementId(statementId)
                .setUpdateBehaviour(PropertyUtils.getProtoUpdateBehaviour(polyphenyStatementProperties.getResultSetConcurrency()))
                .setFetchSize(polyphenyStatementProperties.getFetchSize())
                .setReverseFetch(PropertyUtils.isForwardFetching(polyphenyStatementProperties.getFetchDirection()))
                .setMaxTotalFetchSize(polyphenyStatementProperties.getLargeMaxRows())
                .setDoesEscapeProcessing(polyphenyStatementProperties.isDoesEscapeProcessing())
                .setIsPoolable(polyphenyStatementProperties.isPoolable())
                .build();
    }


    public void executeUnparameterizedStatement(PolyphenyStatementProperties properties, String statement, CallbackQueue<StatementStatus> updateCallback) {
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub(properties.getQueryTimeoutSeconds());
        try {
            stub.executeUnparameterizedStatement(buildUnparameterizedStatement(properties, statement), updateCallback);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public void executeUnparameterizedStatementBatch(PolyphenyStatementProperties properties, List<String> statements, CallbackQueue<StatementBatchStatus> updateCallback) {
        List<UnparameterizedStatement> batch = statements.
                stream()
                .map(s -> buildUnparameterizedStatement(properties, s))
                .collect(Collectors.toList());
        UnparameterizedStatementBatch unparameterizedStatementBatch = UnparameterizedStatementBatch.newBuilder()
                .addAllStatements(batch)
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub(properties.getQueryTimeoutSeconds());
        try {
            stub.executeUnparameterizedStatementBatch(unparameterizedStatementBatch, updateCallback);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    private UnparameterizedStatement buildUnparameterizedStatement(PolyphenyStatementProperties properties, String statement) {
        return UnparameterizedStatement.newBuilder()
                .setStatement(statement)
                .setStatementLanguageName(SQL_LANGUAGE_NAME)
                .setProperties((buildStatementProperties(properties, PolyphenyStatement.NO_STATEMENT_ID)))
                .build();
    }


    public PreparedStatementSignature prepareIndexedStatement(String statement) {
        PreparedStatement preparedStatement = PreparedStatement.newBuilder()
                .setStatement(statement)
                .setStatementLanguageName(SQL_LANGUAGE_NAME)
                .build();
        try {
            return blockingStub.prepareIndexedStatement(preparedStatement);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public StatementResult executeIndexedStatement(int timeout, int statementId, List<TypedValue> values) {
        ParameterList parameterList = buildParameterList(values, statementId);
        ProtoInterfaceGrpc.ProtoInterfaceBlockingStub stub = getBlockingStub(timeout);
        try {
            return stub.executeIndexedStatement(parameterList);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public StatementBatchStatus executeIndexedStatementBatch(int timeout, int statementId, List<List<TypedValue>> parameterBatch) {
        List<ParameterList> parameterLists = parameterBatch.
                stream()
                .map(p -> buildParameterList(p, statementId))
                .collect(Collectors.toList());
        IndexedParameterBatch indexedParameterBatch = IndexedParameterBatch.newBuilder()
                .setStatementId(statementId)
                .addAllParameterLists(parameterLists)
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceBlockingStub stub = getBlockingStub(timeout);
        try {
            return stub.executeIndexedStatementBatch(indexedParameterBatch);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    private ParameterList buildParameterList(List<TypedValue> values, int statementId) {
        return ParameterList.newBuilder()
                .setStatementId(statementId)
                .addAllParameters(ProtoValueSerializer.serializeParameterList(values))
                .build();
    }


    public void commitTransaction() {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();
        try {
            blockingStub.commitTransaction(commitRequest);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public void rollbackTransaction() {
        RollbackRequest rollbackRequest = RollbackRequest.newBuilder().build();
        try {
            blockingStub.rollbackTransaction(rollbackRequest);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public void closeStatement(int statementId) {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId(statementId)
                .build();
        try {
            blockingStub.closeStatement(request);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public Frame fetchResult(int statementId, long offset) {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setStatementId(statementId)
                .setOffset(offset)
                .build();
        try {
            return blockingStub.fetchResult(fetchRequest);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    private String getServerApiVersionString(ConnectionReply connectionReply) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


    public DbmsVersionResponse getDbmsVersion() {
        DbmsVersionRequest dbmsVersionRequest = DbmsVersionRequest.newBuilder().build();
        try {
            return blockingStub.getDbmsVersion(dbmsVersionRequest);
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public List<Database> getDatabases() {
        try {
            return blockingStub.getDatabases(DatabasesRequest.newBuilder().build()).getDatabasesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<ClientInfoPropertyMeta> getClientInfoPropertyMetas() {
        try {
            return blockingStub.getClientInfoPropertyMetas(ClientInfoPropertyMetaRequest.newBuilder().build()).getClientInfoPropertyMetasList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<Type> getTypes() {
        try {
            return blockingStub.getTypes(TypesRequest.newBuilder().build()).getTypesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public String getSqlStringFunctions() {
        try {
            return blockingStub.getSqlStringFunctions(SqlStringFunctionsRequest.newBuilder().build()).getString();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public String getSqlSystemFunctions() {
        try {
            return blockingStub.getSqlSystemFunctions(SqlSystemFunctionsRequest.newBuilder().build()).getString();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public String getSqlTimeDateFunctions() {
        try {
            return blockingStub.getSqlTimeDateFunctions(SqlTimeDateFunctionsRequest.newBuilder().build()).getString();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public String getSqlNumericFunctions() {
        try {
            return blockingStub.getSqlNumericFunctions(SqlNumericFunctionsRequest.newBuilder().build()).getString();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public String getSqlKeywords() {
        try {
            return blockingStub.getSqlKeywords(SqlKeywordsRequest.newBuilder().build()).getString();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public void setConnectionProperties(PolyphenyConnectionProperties connectionProperties) {
        try {
            blockingStub.updateConnectionProperties(buildConnectionProperties(connectionProperties));
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public void setStatementProperties(PolyphenyStatementProperties statementProperties, int statementId) {
        try {
            blockingStub.updateStatementProperties(buildStatementProperties(statementProperties, statementId));
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<Procedure> searchProcedures(String languageName, String procedureNamePattern) {
        ProceduresRequest.Builder requestBuilder = ProceduresRequest.newBuilder();
        requestBuilder.setLanguage(languageName);
        Optional.ofNullable(procedureNamePattern).ifPresent(requestBuilder::setProcedureNamePattern);
        try {
            return blockingStub.searchProcedures(requestBuilder.build()).getProceduresList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public Map<String, String> getClientInfoProperties() {
        try {
            return blockingStub.getClientInfoProperties(ClientInfoPropertiesRequest.newBuilder().build()).getPropertiesMap();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }


    public List<Namespace> searchNamespaces(String schemaPattern, String protoNamespaceType) {
        NamespacesRequest.Builder requestBuilder = NamespacesRequest.newBuilder();
        Optional.ofNullable(schemaPattern).ifPresent(requestBuilder::setNamespacePattern);
        Optional.ofNullable(protoNamespaceType).ifPresent(requestBuilder::setNamespaceType);
        try {
            return blockingStub.searchNamespaces(requestBuilder.build()).getNamespacesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<Entity> searchEntities(String namespace, String entityNamePattern) {
        EntitiesRequest.Builder requestBuilder = EntitiesRequest.newBuilder();
        requestBuilder.setNamespaceName(namespace);
        Optional.ofNullable(entityNamePattern).ifPresent(requestBuilder::setEntityPattern);
        try {
            return blockingStub.searchEntities(requestBuilder.build()).getEntitiesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<TableType> getTablesTypes() {
        try {
            return blockingStub.getTableTypes(TableTypesRequest.newBuilder().build()).getTableTypesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public Namespace getNamespace(String namespaceName) {
        NamespaceRequest.Builder requestBuilder = NamespaceRequest.newBuilder();
        requestBuilder.setNamespaceName(namespaceName);
        try {
            return blockingStub.getNamespace(requestBuilder.build());
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<UserDefinedType> getUserDefinedTypes() {
        UserDefinedTypesRequest.Builder requestBuilder = UserDefinedTypesRequest.newBuilder();
        try {
            return blockingStub.getUserDefinedTypes(requestBuilder.build()).getUserDefinedTypesList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public void setClientInfoProperties(Properties properties) {
        ClientInfoProperties.Builder requestBuilder = ClientInfoProperties.newBuilder();
        properties.stringPropertyNames()
                .forEach(s -> requestBuilder.putProperties(s, properties.getProperty(s)));
        try {
            blockingStub.setClientInfoProperties(requestBuilder.build());
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }

    public List<Function> searchFunctions(String languaheName, String functionCategory) {
        FunctionsRequest functionsRequest = FunctionsRequest.newBuilder()
                .setQueryLanguage(languaheName)
                .setFunctionCategory(functionCategory)
                .build();
        try {
            return blockingStub.searchFunctions(functionsRequest).getFunctionsList();
        } catch (Exception e) {
            throw new ProtoInterfaceServiceException(e.getMessage());
        }
    }
}
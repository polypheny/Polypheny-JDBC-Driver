package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import org.polypheny.jdbc.properties.PolyphenyConnectionProperties;
import org.polypheny.jdbc.proto.*;
import org.polypheny.jdbc.serialisation.ProtoValueSerializer;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
    private static final String SQL_LANGUAGE_NAME = "sql";
    private final ProtoInterfaceGrpc.ProtoInterfaceBlockingStub blockingStub;
    private final ProtoInterfaceGrpc.ProtoInterfaceStub asyncStub;
    private final String clientUUID;


    public ProtoInterfaceClient(String target) throws SQLException {
        this.clientUUID = UUID.randomUUID().toString();
        try {
            Channel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create())
                    .intercept(new ClientMetaInterceptor(clientUUID))
                    .build();
            this.blockingStub = ProtoInterfaceGrpc.newBlockingStub(channel);
            this.asyncStub = ProtoInterfaceGrpc.newStub(channel);
        } catch (Exception e) {
            throw new SQLException("Connection failed: " + e.getMessage());
        }
    }


    private ProtoInterfaceGrpc.ProtoInterfaceBlockingStub getBlockingStub(int timeout) {
        if (timeout == 0) {
            return blockingStub;
        }
        return blockingStub.withDeadlineAfter(timeout, TimeUnit.SECONDS);
    }


    private ProtoInterfaceGrpc.ProtoInterfaceStub getAsyncStub(int timeout) {
        if (timeout == 0) {
            return asyncStub;
        }
        return asyncStub.withDeadlineAfter(timeout, TimeUnit.SECONDS);
    }


    public boolean checkConnection(int timeout) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            blockingStub.withDeadlineAfter(timeout, TimeUnit.SECONDS).checkConnection(request);
        } catch (Exception e) {
            return false;
        }
        return true;
    }


    public List<String> requestSupportedLanguages() {
        LanguageRequest languageRequest = LanguageRequest.newBuilder().build();
        return blockingStub.getSupportedLanguages(languageRequest).getLanguageNamesList();
    }


    public void register(PolyphenyConnectionProperties connectionProperties) {


        ConnectionRequest connectionRequest = ConnectionRequest.newBuilder()
                .setMajorApiVersion(MAJOR_API_VERSION)
                .setMinorApiVersion(MINOR_API_VERSION)
                .setClientUuid(clientUUID)
                .setConnectionProperties(buildConnectionProperties(connectionProperties))
                .build();
        ConnectionReply connectionReply = blockingStub.connect(connectionRequest);
        if (!connectionReply.getIsCompatible()) {
            throw new ProtoInterfaceServiceException("client version " + getClientApiVersionString()
                    + "not compatible with server version " + getServerApiVersionString(connectionReply) + ".");
        }
    }

    private ConnectionProperties buildConnectionProperties(PolyphenyConnectionProperties polyphenyConnectionProperties) {
        ConnectionProperties.Builder propertiesBuilder = ConnectionProperties.newBuilder();
        Optional.ofNullable(polyphenyConnectionProperties.getUsername()).ifPresent(propertiesBuilder::setUsername);
        Optional.ofNullable(polyphenyConnectionProperties.getPassword()).ifPresent(propertiesBuilder::setPassword);
        Optional.ofNullable(polyphenyConnectionProperties.getNamespaceName()).ifPresent(propertiesBuilder::setNamespaceName);
        return propertiesBuilder
                .setIsAutoCommit(polyphenyConnectionProperties.isAutoCommit())
                .setIsReadOnly(polyphenyConnectionProperties.isReadOnly())
                .setHoldability(polyphenyConnectionProperties.getProtoHoldability())
                .setIsolation(polyphenyConnectionProperties.getProtoIsolation())
                .setNetworkTimeout(polyphenyConnectionProperties.getNetworkTimeout())
                .build();
    }


    public void executeUnparameterizedStatement(int timeout, String statement, CallbackQueue<StatementStatus> updateCallback) {
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub(timeout);
        stub.executeUnparameterizedStatement(buildUnparameterizedStatement(statement), updateCallback);
    }


    public void executeUnparameterizedStatementBatch(int timeout, List<String> statements, CallbackQueue<StatementBatchStatus> updateCallback) {
        List<UnparameterizedStatement> batch = statements.
                stream()
                .map(this::buildUnparameterizedStatement)
                .collect(Collectors.toList());
        UnparameterizedStatementBatch unparameterizedStatementBatch = UnparameterizedStatementBatch.newBuilder()
                .addAllStatements(batch)
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub(timeout);
        stub.executeUnparameterizedStatementBatch(unparameterizedStatementBatch, updateCallback);
    }


    private UnparameterizedStatement buildUnparameterizedStatement(String statement) {
        return UnparameterizedStatement.newBuilder()
                .setStatement(statement)
                .setStatementLanguageName(SQL_LANGUAGE_NAME)
                .build();
    }


    public PreparedStatementSignature prepareIndexedStatement(String statement) {
        PreparedStatement preparedStatement = PreparedStatement.newBuilder()
                .setStatement(statement)
                .setStatementLanguageName(SQL_LANGUAGE_NAME)
                .build();
        return blockingStub.prepareIndexedStatement(preparedStatement);
    }


    public StatementResult executeIndexedStatement(int timeout, int statementId, List<TypedValue> values) {
        ParameterList parameterList = buildParameterList(values, statementId);
        ProtoInterfaceGrpc.ProtoInterfaceBlockingStub stub = getBlockingStub(timeout);
        return stub.executeIndexedStatement(parameterList);
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
        return stub.executeIndexedStatementBatch(indexedParameterBatch);
    }


    private ParameterList buildParameterList(List<TypedValue> values, int statementId) {
        return ParameterList.newBuilder()
                .setStatementId(statementId)
                .addAllParameters(ProtoValueSerializer.serializeParameterList(values))
                .build();
    }


    public void commitTransaction() {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();
        blockingStub.commitTransaction(commitRequest);
    }

    public void rollbackTransaction() {
        RollbackRequest rollbackRequest = RollbackRequest.newBuilder().build();
        blockingStub.rollbackTransaction(rollbackRequest);
    }


    public void closeStatement(int statementId) {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId(statementId)
                .build();
        blockingStub.closeStatement(request);
    }


    public Frame fetchResult(int statementId, long offset, int fetchSize) {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setStatementId(statementId)
                .setOffset(offset)
                .setFetchSize(fetchSize)
                .build();
        return blockingStub.fetchResult(fetchRequest);
    }


    private String getServerApiVersionString(ConnectionReply connectionReply) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


    public DbmsVersionResponse getDbmsVersion() {
        DbmsVersionRequest dbmsVersionRequest = DbmsVersionRequest.newBuilder().build();
        return blockingStub.getDbmsVersion(dbmsVersionRequest);
    }


    public TablesResponse getTables(String namespacePattern, String tablePattern, String[] types) {
        TablesRequest.Builder requestBuilder = TablesRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        Optional.ofNullable(tablePattern).ifPresent(requestBuilder::setTablePattern);
        Optional.ofNullable(types).ifPresent(t -> requestBuilder.addAllTableTypes(Arrays.asList(t)));
        return blockingStub.getTables(requestBuilder.build());
    }


    public TableTypesResponse getTablesTypes() {
        return blockingStub.getTableTypes(TableTypesRequest.newBuilder().build());
    }


    public NamespacesResponse getNamespaces(String namespacePattern) {
        NamespacesRequest.Builder requestBuilder = NamespacesRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        return blockingStub.getNamespaces(requestBuilder.build());
    }


    public ColumnsResponse getColumns(String namespacePattern, String tablePattern, String columnPattern) {
        ColumnsRequest.Builder requestBuilder = ColumnsRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        Optional.ofNullable(tablePattern).ifPresent(requestBuilder::setTablePattern);
        Optional.ofNullable(columnPattern).ifPresent(requestBuilder::setColumnPattern);
        return blockingStub.getColumns(requestBuilder.build());
    }


    public PrimaryKeysResponse getPrimaryKeys(String namespacePattern, String tablePattern) {
        PrimaryKeysRequest.Builder requestBuilder = PrimaryKeysRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        requestBuilder.setTablePattern(tablePattern);
        return blockingStub.getPrimaryKeys(requestBuilder.build());
    }


    public DatabasesResponse getDatabases() {
        return blockingStub.getDatabases(DatabasesRequest.newBuilder().build());
    }


    public ImportedKeysResponse getImportedKeys(String namespacePattern, String tablePattern) {
        ImportedKeysRequest.Builder requestBuilder = ImportedKeysRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        requestBuilder.setTablePattern(tablePattern);
        return blockingStub.getImportedKeys(requestBuilder.build());
    }


    public ExportedKeysResponse getExportedKeys(String namespacePattern, String tablePattern) {
        ExportedKeysRequest.Builder requestBuilder = ExportedKeysRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        requestBuilder.setTablePattern(tablePattern);
        return blockingStub.getExportedKeys(requestBuilder.build());
    }


    public TypesResponse getTypes() {
        return blockingStub.getTypes(TypesRequest.newBuilder().build());
    }


    public IndexesResponse getIndexes(String namespacePattern, String tablePattern, boolean unique) {
        IndexesRequest.Builder requestBuilder = IndexesRequest.newBuilder();
        Optional.ofNullable(namespacePattern).ifPresent(requestBuilder::setNamespacePattern);
        Optional.ofNullable(tablePattern).ifPresent(requestBuilder::setTablePattern);
        requestBuilder.setUnique(unique);
        return blockingStub.getIndexes(requestBuilder.build());
    }

    public void setConnectionProperties(PolyphenyConnectionProperties connectionProperties) {

    }
}
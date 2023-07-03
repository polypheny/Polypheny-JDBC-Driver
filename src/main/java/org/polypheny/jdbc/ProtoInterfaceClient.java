package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.polypheny.jdbc.proto.CloseStatementRequest;
import org.polypheny.jdbc.proto.CommitRequest;
import org.polypheny.jdbc.proto.ConnectionCheckRequest;
import org.polypheny.jdbc.proto.ConnectionReply;
import org.polypheny.jdbc.proto.ConnectionRequest;
import org.polypheny.jdbc.proto.FetchRequest;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.ParameterSet;
import org.polypheny.jdbc.proto.PreparedStatement;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.proto.ProtoInterfaceGrpc;
import org.polypheny.jdbc.proto.StatementBatchStatus;
import org.polypheny.jdbc.proto.StatementResult;
import org.polypheny.jdbc.proto.StatementStatus;
import org.polypheny.jdbc.proto.UnparameterizedStatement;
import org.polypheny.jdbc.proto.UnparameterizedStatementBatch;
import org.polypheny.jdbc.types.ProtoValueSerializer;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.jdbc.utils.NamedParameterUtils;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
    private static final String SQL_LANGUAGE_NAME = "sql";
    private final ProtoInterfaceGrpc.ProtoInterfaceBlockingStub blockingStub;
    private final ProtoInterfaceGrpc.ProtoInterfaceStub asyncStub;
    private final String clientUUID;


    public ProtoInterfaceClient( String target ) {
        this.clientUUID = UUID.randomUUID().toString();
        Channel channel = Grpc.newChannelBuilder( target, InsecureChannelCredentials.create() )
                .intercept( new ClientMetaInterceptor( clientUUID ) )
                .build();
        this.blockingStub = ProtoInterfaceGrpc.newBlockingStub( channel );
        this.asyncStub = ProtoInterfaceGrpc.newStub( channel );

    }

    private ProtoInterfaceGrpc.ProtoInterfaceBlockingStub getBlockingStub(int timeout) {
        if (timeout == 0) {
            return blockingStub;
        }
        return blockingStub.withDeadlineAfter( timeout, TimeUnit.SECONDS );
    }

    private ProtoInterfaceGrpc.ProtoInterfaceStub getAsyncStub(int timeout) {
        if (timeout == 0) {
            return asyncStub;
        }
        return asyncStub.withDeadlineAfter( timeout, TimeUnit.SECONDS );
    }


    public boolean checkConnection( int timeout ) {
        ConnectionCheckRequest request = ConnectionCheckRequest.newBuilder().build();
        try {
            /* ConnectionCheckResponses are empty messages */
            blockingStub.withDeadlineAfter( timeout, TimeUnit.SECONDS ).connectionCheck( request );
        } catch ( Exception e ) {
            return false;
        }
        return true;
    }


    public void connect( Map<String, String> properties ) {
        ConnectionRequest connectionRequest = ConnectionRequest.newBuilder()
                .setMajorApiVersion( MAJOR_API_VERSION )
                .setMinorApiVersion( MINOR_API_VERSION )
                .setClientUuid( clientUUID )
                .putAllConnectionProperties( properties )
                .build();
        ConnectionReply connectionReply = blockingStub.connect( connectionRequest );
        if ( !connectionReply.getIsCompatible() ) {
            throw new ProtoInterfaceServiceException( "client version " + getClientApiVersionString()
                    + "not compatible with server version " + getServerApiVersionString( connectionReply ) + "." );
        }
    }


    public void executeUnparameterizedStatement( int timeout, String statement, CallbackQueue<StatementStatus> updateCallback ) {
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub( timeout );
        stub.executeUnparameterizedStatement( buildUnparameterizedStatement( statement ), updateCallback );
    }


    public void executeUnparameterizedStatementBatch( int timeout, List<String> statements, CallbackQueue<StatementBatchStatus> updateCallback ) {
        List<UnparameterizedStatement> batch = statements.
                stream()
                .map( this::buildUnparameterizedStatement )
                .collect( Collectors.toList() );
        UnparameterizedStatementBatch unparameterizedStatementBatch = UnparameterizedStatementBatch.newBuilder()
                .addAllStatements( batch )
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceStub stub = getAsyncStub( timeout );
        stub.executeUnparameterizedStatementBatch( unparameterizedStatementBatch, updateCallback );
    }


    private UnparameterizedStatement buildUnparameterizedStatement( String statement ) {
        return UnparameterizedStatement.newBuilder()
                .setStatement( statement )
                .setStatementLanguageName( SQL_LANGUAGE_NAME )
                .build();
    }


    public PreparedStatementSignature prepareStement( String statement ) {
        PreparedStatement preparedStatement = PreparedStatement.newBuilder()
                .setStatement( statement )
                .setStatementLanguageName( SQL_LANGUAGE_NAME )
                .build();
        return blockingStub.prepareStatement( preparedStatement );

    }


    public StatementResult executePreparedStatement( int timeout, int statementId, List<TypedValue> parameters ) {
        Map<String, TypedValue> typedValueMap = NamedParameterUtils.convertToParameterMap( parameters );
        return executePreparedStatement( timeout, statementId, typedValueMap );
    }


    public StatementResult executePreparedStatement( int timeout, int statementId, Map<String, TypedValue> parameters ) {
        ParameterSet parameterSet = ParameterSet.newBuilder()
                .setStatementId( statementId )
                .putAllValues( ProtoValueSerializer.serializeParameterMap( parameters ) )
                .build();
        ProtoInterfaceGrpc.ProtoInterfaceBlockingStub stub = getBlockingStub(timeout);
        return stub.executePreparedStatement( parameterSet );
    }


    public void commitTransaction() {
        CommitRequest commitRequest = CommitRequest.newBuilder().build();
        blockingStub.commitTransaction( commitRequest );
    }


    public void closeStatement( int statementId ) {
        CloseStatementRequest request = CloseStatementRequest.newBuilder()
                .setStatementId( statementId )
                .build();
        blockingStub.closeStatement( request );
    }


    public Frame fetchResult( int statementId, long offset, int fetchSize ) {
        FetchRequest fetchRequest = FetchRequest.newBuilder()
                .setStatementId( statementId )
                .setOffset( offset )
                .setFetchSize( fetchSize )
                .build();
        return blockingStub.fetchResult( fetchRequest );
    }


    private String getServerApiVersionString( ConnectionReply connectionReply ) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }

}
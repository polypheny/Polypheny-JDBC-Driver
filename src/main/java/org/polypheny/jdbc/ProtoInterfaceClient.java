package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import java.util.Map;
import java.util.UUID;
import org.polypheny.jdbc.proto.CloseStatementRequest;
import org.polypheny.jdbc.proto.CommitRequest;
import org.polypheny.jdbc.proto.ConnectionReply;
import org.polypheny.jdbc.proto.ConnectionRequest;
import org.polypheny.jdbc.proto.FetchRequest;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.ProtoInterfaceGrpc;
import org.polypheny.jdbc.proto.UnparameterizedStatement;
import org.polypheny.jdbc.utils.StatementStatusQueue;

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


    public void executeUnparameterizedStatement( String statement, StatementStatusQueue updateCallback ) {
        UnparameterizedStatement.Builder statementBuilder = UnparameterizedStatement.newBuilder()
                .setStatementLanguageName( SQL_LANGUAGE_NAME )
                .setStatement( statement );
        asyncStub.executeUnparameterizedStatement( statementBuilder.build(), updateCallback );
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
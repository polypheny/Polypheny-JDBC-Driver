package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import org.polypheny.jdbc.proto.*;

import java.util.Map;
import java.util.UUID;

public class ProtoInterfaceClient {

    private static final int MAJOR_API_VERSION = 2;
    private static final int MINOR_API_VERSION = 0;
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
                .setClientUUID( clientUUID )
                .putAllConnectionProperties( properties )
                .build();
        ConnectionReply connectionReply = blockingStub.connect( connectionRequest );
        if ( !connectionReply.getIsCompatible() ) {
            throw new ProtoInterfaceServiceException( "client version " + getClientApiVersionString()
                    + "not compatible with server version " + getServerApiVersionString( connectionReply ) + "." );
        }
    }


    public QueryResult executeSimpleQuery( String sql ) {
        SimpleSqlQuery query = SimpleSqlQuery.newBuilder()
                .setQuery( sql )
                .build();
        return blockingStub.executeSimpleSqlQuery( query );
    }


    private String getServerApiVersionString( ConnectionReply connectionReply ) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


}
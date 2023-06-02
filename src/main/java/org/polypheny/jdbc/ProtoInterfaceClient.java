package org.polypheny.jdbc;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import lombok.Getter;
import org.polypheny.jdbc.proto.*;

import java.util.Map;
import java.util.UUID;

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


    private String getServerApiVersionString( ConnectionReply connectionReply ) {
        return connectionReply.getMajorApiVersion() + "." + connectionReply.getMinorApiVersion();

    }


    private static String getClientApiVersionString() {
        return MAJOR_API_VERSION + "." + MINOR_API_VERSION;
    }


    private static String getSqlLanguageName() {
        return SQL_LANGUAGE_NAME;
    }

}
package org.polypheny.jdbc.test;

import io.grpc.Channel;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import org.polypheny.jdbc.proto.RequestsA;
import org.polypheny.jdbc.proto.RequestsB;
import org.polypheny.jdbc.proto.TestInterfaceGrpc;

public class TestClient {

    private final TestInterfaceGrpc.TestInterfaceBlockingStub blockingStub;
    private final TestInterfaceGrpc.TestInterfaceStub asyncStub;


    public TestClient() {
        Channel channel = Grpc.newChannelBuilder( "localhost:60500", InsecureChannelCredentials.create() ).build();
        this.blockingStub = TestInterfaceGrpc.newBlockingStub( channel );
        this.asyncStub = TestInterfaceGrpc.newStub( channel );
    }


    public int callA() {
        return blockingStub.callA( RequestsA.newBuilder().build() ).getAnswer();
    }


    public int callB() {
        return blockingStub.callB( RequestsB.newBuilder().build() ).getAnswer();
    }

}

package org.polypheny.jdbc.test;

import io.grpc.Grpc;
import io.grpc.InsecureServerCredentials;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import org.polypheny.jdbc.proto.RequestsA;
import org.polypheny.jdbc.proto.RequestsB;
import org.polypheny.jdbc.proto.TResponse;
import org.polypheny.jdbc.proto.TestInterfaceGrpc;

public class TestServer {

    private final Server server;


    public TestServer() throws IOException {
        ServerBuilder<?> serverBuilder = Grpc.newServerBuilderForPort( 60500, InsecureServerCredentials.create() );
        server = serverBuilder.addService( new TestService() ).build();
        server.start();
    }


    public static class TestService extends TestInterfaceGrpc.TestInterfaceImplBase {

        @Override
        public void callA( RequestsA request, StreamObserver<TResponse> responseObserver ) {
            for ( int i = 0; i < 20; i++ ) {
                try {
                    System.out.println( "Call A counter @ " + i );
                    Thread.sleep( 1000 );
                } catch ( InterruptedException ex ) {
                    System.out.println( ex.getMessage() );
                }
            }
            responseObserver.onNext( TResponse.newBuilder().build() );
        }


        @Override
        public void callB( RequestsB request, StreamObserver<TResponse> responseObserver ) {
            for ( int i = 0; i < 20; i++ ) {
                try {
                    System.out.println( "Call B counter @ " + i );
                    Thread.sleep( 1000 );
                } catch ( InterruptedException ex ) {
                    System.out.println( ex.getMessage() );
                }
            }
            responseObserver.onNext( TResponse.newBuilder().build() );
        }

    }

}

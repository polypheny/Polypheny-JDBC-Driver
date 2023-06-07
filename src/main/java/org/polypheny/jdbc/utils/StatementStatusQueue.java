package org.polypheny.jdbc.utils;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.LinkedBlockingQueue;
import org.polypheny.jdbc.proto.StatementStatus;

public class StatementStatusQueue extends LinkedBlockingQueue<StatementStatus> implements StreamObserver<StatementStatus> {

    public StatementStatusQueue() {
        super();
    }


    @Override
    public void onNext( StatementStatus status ) {
        try {
            put( status );
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public void onError( Throwable t ) {

    }


    @Override
    public void onCompleted() {

    }

}
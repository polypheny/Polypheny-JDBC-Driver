package org.polypheny.jdbc.utils;

import io.grpc.stub.StreamObserver;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Getter;
import org.polypheny.jdbc.proto.StatementStatus;

public class StatementStatusQueue extends LinkedBlockingQueue<StatementStatus> implements StreamObserver<StatementStatus> {
    boolean isCompleted;

    public StatementStatusQueue() {
        super();
        this.isCompleted = false;
    }

    public void awaitCompletion() {
        try {
            synchronized(this) {
                while (!isCompleted) {
                    this.wait();
                }
            }
        } catch ( InterruptedException e ) {
            throw new RuntimeException( e );
        }
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
        synchronized ( this ) {
            isCompleted = true;
            this.notify();
        }
    }

}
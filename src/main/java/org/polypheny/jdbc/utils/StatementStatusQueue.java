package org.polypheny.jdbc.utils;

import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.StatementStatus;

public class StatementStatusQueue implements StreamObserver<StatementStatus> {

    private final Lock queueLock = new ReentrantLock();
    private final Condition hasNext = queueLock.newCondition();
    private final Condition isCompleted = queueLock.newCondition();
    private LinkedList<StatementStatus> statusQueue;
    private Throwable propagatedException;
    private boolean bIsCompleted;


    public StatementStatusQueue() {
        this.statusQueue = new LinkedList<>();
        this.bIsCompleted = false;
    }


    public void awaitCompletion() throws InterruptedException {
        queueLock.lock();
        while (!bIsCompleted) {
            isCompleted.await();
        }
    }


    public StatementStatus takeNext() throws InterruptedException, ProtoInterfaceServiceException {
        queueLock.lock();
        while ( statusQueue.isEmpty() ) {
            hasNext.await();
            throwReceivedException();
        }
        StatementStatus status = statusQueue.remove();
        queueLock.unlock();
        return status;
    }


    private void throwReceivedException() throws ProtoInterfaceServiceException {
        if ( propagatedException != null ) {
            throw new ProtoInterfaceServiceException( propagatedException.getLocalizedMessage() );
        }
    }


    @Override
    public void onNext( StatementStatus status ) {
        queueLock.lock();
        statusQueue.add( status );
        hasNext.signal();
        queueLock.unlock();
    }


    @Override
    public void onError( Throwable propagatedException ) {
        queueLock.lock();
        this.propagatedException = propagatedException;
        hasNext.signal();
        queueLock.unlock();
    }


    @Override
    public void onCompleted() {
        queueLock.lock();
        bIsCompleted = true;
        isCompleted.signal();
        queueLock.unlock();
    }

}
package org.polypheny.jdbc.utils;

import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.polypheny.jdbc.ProtoInterfaceServiceException;

public class CallbackQueue<T> implements StreamObserver<T> {

    private final Lock queueLock = new ReentrantLock();
    private final Condition hasNext = queueLock.newCondition();
    private final Condition isCompleted = queueLock.newCondition();
    private LinkedList<T> messageQueue;
    private Throwable propagatedException;
    private boolean bIsCompleted;


    public CallbackQueue() {
        this.messageQueue = new LinkedList<>();
        this.bIsCompleted = false;
    }


    public void awaitCompletion() throws InterruptedException {
        queueLock.lock();
        while ( !bIsCompleted ) {
            isCompleted.await();
        }
    }


    public T takeNext() throws Throwable {
        queueLock.lock();
        while ( messageQueue.isEmpty() ) {
            hasNext.await();
            throwReceivedException();
        }
        T message = messageQueue.remove();
        queueLock.unlock();
        return message;
    }


    private void throwReceivedException() throws Throwable {
        if ( propagatedException != null ) {
            throw propagatedException;
        }
    }


    @Override
    public void onNext( T message ) {
        queueLock.lock();
        messageQueue.add( message );
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
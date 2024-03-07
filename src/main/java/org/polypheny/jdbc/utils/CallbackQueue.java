package org.polypheny.jdbc.utils;

import java.util.LinkedList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.ProtoInterfaceErrors;

public class CallbackQueue<T> {

    private final Lock queueLock = new ReentrantLock();
    private final Condition hasNext = queueLock.newCondition();
    private final Condition isCompleted = queueLock.newCondition();
    private LinkedList<T> messageQueue;
    private ProtoInterfaceServiceException propagatedException;
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


    public T takeNext() throws ProtoInterfaceServiceException {
        queueLock.lock();
        while ( messageQueue.isEmpty() ) {
            try {
                hasNext.await();
            } catch ( InterruptedException e ) {
                throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting next response failed.", e );
            }
            throwReceivedException();
        }
        T message = messageQueue.remove();
        queueLock.unlock();
        return message;
    }


    private void throwReceivedException() throws ProtoInterfaceServiceException {
        if ( propagatedException != null ) {
            throw propagatedException;
        }
    }


    public void onNext( T message ) {
        queueLock.lock();
        messageQueue.add( message );
        hasNext.signal();
        queueLock.unlock();
    }


    public void onError( Throwable propagatedException ) {
        queueLock.lock();
        this.propagatedException = new ProtoInterfaceServiceException( propagatedException );
        hasNext.signal();
        queueLock.unlock();
    }


    public void onCompleted() {
        queueLock.lock();
        bIsCompleted = true;
        isCompleted.signal();
        queueLock.unlock();
    }

}
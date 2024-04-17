package org.polypheny.jdbc.utils;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import org.polypheny.db.protointerface.proto.Response;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;

public class CallbackQueue<T> {

    private final Lock queueLock = new ReentrantLock();
    private final Condition hasNext = queueLock.newCondition();
    private final Condition isCompleted = queueLock.newCondition();
    private boolean bIsCompleted = false;
    private final Queue<T> messageQueue = new LinkedList<>();
    private final Function<Response, T> extractResponse;
    private PrismInterfaceServiceException propagatedException;


    public CallbackQueue( Function<Response, T> extractResponse ) {
        this.extractResponse = extractResponse;
    }


    public void awaitCompletion() throws InterruptedException {
        queueLock.lock();
        while ( !bIsCompleted ) {
            isCompleted.await();
        }
    }


    public T takeNext() throws PrismInterfaceServiceException {
        queueLock.lock();
        while ( messageQueue.isEmpty() ) {
            try {
                hasNext.await();
            } catch ( InterruptedException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting next response failed.", e );
            }
            throwReceivedException();
        }
        T message = messageQueue.remove();
        queueLock.unlock();
        return message;
    }


    private void throwReceivedException() throws PrismInterfaceServiceException {
        if ( propagatedException != null ) {
            throw propagatedException;
        }
    }


    public void onNext( Response message ) {
        queueLock.lock();
        messageQueue.add( extractResponse.apply( message ) );
        hasNext.signal();
        queueLock.unlock();
    }


    public void onError( Throwable propagatedException ) {
        queueLock.lock();
        this.propagatedException = new PrismInterfaceServiceException( propagatedException );
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

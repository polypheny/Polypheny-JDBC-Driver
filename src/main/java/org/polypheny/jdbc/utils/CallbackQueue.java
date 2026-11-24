/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.jdbc.utils;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.Response;

public class CallbackQueue<T> {

    private final Lock queueLock = new ReentrantLock();
    private final Condition hasNext = queueLock.newCondition();
    private final Condition isCompleted = queueLock.newCondition();
    private boolean bIsCompleted = false;
    private final Queue<T> messageQueue = new LinkedList<>();
    private final Function<Response, T> extractResponse;
    private final long timeoutMillis;
    private final long deadline;
    private final LongSupplier nanoClock;
    private PrismInterfaceServiceException propagatedException;


    public CallbackQueue( Function<Response, T> extractResponse ) {
        this( extractResponse, 0 );
    }


    public CallbackQueue( Function<Response, T> extractResponse, int timeoutMillis ) {
        this( extractResponse, (long) timeoutMillis );
    }


    public CallbackQueue( Function<Response, T> extractResponse, long timeoutMillis ) {
        this( extractResponse, timeoutMillis, System::nanoTime );
    }


    public CallbackQueue( Function<Response, T> extractResponse, long timeoutMillis, long deadlineNanos ) {
        this( extractResponse, timeoutMillis, deadlineNanos, System::nanoTime );
    }


    CallbackQueue( Function<Response, T> extractResponse, long timeoutMillis, LongSupplier nanoClock ) {
        this( extractResponse, timeoutMillis, nanoClock.getAsLong() + TimeUnit.MILLISECONDS.toNanos( timeoutMillis ), nanoClock );
    }


    private CallbackQueue( Function<Response, T> extractResponse, long timeoutMillis, long deadlineNanos, LongSupplier nanoClock ) {
        this.extractResponse = extractResponse;
        this.timeoutMillis = timeoutMillis;
        this.nanoClock = nanoClock;
        this.deadline = deadlineNanos;
    }


    public long getTimeoutMillis() {
        return timeoutMillis;
    }


    public long getDeadlineNanos() {
        return deadline;
    }


    public void awaitCompletion() throws InterruptedException {
        awaitCompletionFailure();
    }


    public void awaitSuccessfulCompletion() throws InterruptedException, PrismInterfaceServiceException {
        PrismInterfaceServiceException failure = awaitCompletionFailure();
        if ( failure != null ) {
            throw failure;
        }
    }


    private PrismInterfaceServiceException awaitCompletionFailure() throws InterruptedException {
        queueLock.lock();
        try {
            while ( !bIsCompleted && propagatedException == null ) {
                if ( !awaitSignal( isCompleted ) ) {
                    return recordTimeout();
                }
            }
            return propagatedException;
        } finally {
            queueLock.unlock();
        }
    }


    public T takeNext() throws PrismInterfaceServiceException {
        queueLock.lock();
        try {
            while ( messageQueue.isEmpty() && propagatedException == null && !bIsCompleted ) {
                if ( !awaitSignal( hasNext ) ) {
                    throw recordTimeout();
                }
            }
            if ( !messageQueue.isEmpty() ) {
                return messageQueue.remove();
            }
            if ( propagatedException != null ) {
                throw propagatedException;
            }
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Prism response ended before the expected result." );
        } catch ( InterruptedException e ) {
            Thread.currentThread().interrupt();
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DRIVER_THREADING_ERROR, "Awaiting next response failed.", e );
        } finally {
            queueLock.unlock();
        }
    }


    private boolean awaitSignal( Condition condition ) throws InterruptedException {
        if ( timeoutMillis == 0 ) {
            condition.await();
            return true;
        }
        long remaining = deadline - nanoClock.getAsLong();
        if ( remaining <= 0 ) {
            return false;
        }
        condition.awaitNanos( remaining );
        return true;
    }


    private PrismInterfaceServiceException recordTimeout() {
        propagatedException = new PrismInterfaceServiceException( new TimeoutException( "No complete Prism response within " + timeoutMillis + " ms." ) );
        signalTerminalState();
        return propagatedException;
    }


    private boolean acceptsCallback() {
        if ( bIsCompleted || propagatedException != null ) {
            return false;
        }
        if ( timeoutMillis != 0 && deadline - nanoClock.getAsLong() <= 0 ) {
            recordTimeout();
            return false;
        }
        return true;
    }


    private void signalTerminalState() {
        hasNext.signalAll();
        isCompleted.signalAll();
    }


    public void onNext( Response message ) {
        queueLock.lock();
        try {
            if ( !acceptsCallback() ) {
                return;
            }
            messageQueue.add( extractResponse.apply( message ) );
            if ( message.getLast() ) {
                // A terminal frame arrives and completes the queue at the same instant.
                bIsCompleted = true;
                signalTerminalState();
            } else {
                hasNext.signal();
            }
        } finally {
            queueLock.unlock();
        }
    }


    public void onError( Throwable propagatedException ) {
        queueLock.lock();
        try {
            if ( acceptsCallback() ) {
                this.propagatedException = propagatedException instanceof PrismInterfaceServiceException
                        ? (PrismInterfaceServiceException) propagatedException
                        : new PrismInterfaceServiceException( propagatedException );
                signalTerminalState();
            }
        } finally {
            queueLock.unlock();
        }
    }


    public void onCompleted() {
        queueLock.lock();
        try {
            if ( acceptsCallback() ) {
                bIsCompleted = true;
                signalTerminalState();
            }
        } finally {
            queueLock.unlock();
        }
    }

}

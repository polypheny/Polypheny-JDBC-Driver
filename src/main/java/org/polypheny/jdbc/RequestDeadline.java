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

package org.polypheny.jdbc;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.polypheny.jdbc.utils.CallbackQueue;
import org.polypheny.prism.Response;

final class RequestDeadline {

    final long timeoutMillis;
    private final long startedAtNanos;
    private final long deadlineNanos;
    private final long serverQueryNetworkTimeoutMillis;
    private final long serverQueryTimeoutMillis;
    private long transactionGeneration = -1;


    RequestDeadline( long timeoutMillis ) {
        this( timeoutMillis, System.nanoTime(), -1, 0 );
    }


    private RequestDeadline( long timeoutMillis, long startedAtNanos, long serverQueryNetworkTimeoutMillis, long serverQueryTimeoutMillis ) {
        this.timeoutMillis = timeoutMillis;
        this.startedAtNanos = startedAtNanos;
        this.deadlineNanos = startedAtNanos + TimeUnit.MILLISECONDS.toNanos( timeoutMillis );
        this.serverQueryNetworkTimeoutMillis = serverQueryNetworkTimeoutMillis;
        this.serverQueryTimeoutMillis = serverQueryTimeoutMillis;
    }


    RequestDeadline withServerQueryTimeout( int networkTimeout, long queryTimeout ) {
        return new RequestDeadline( timeoutMillis, startedAtNanos, networkTimeout, queryTimeout );
    }


    boolean usesServerQueryTimeout() {
        return serverQueryNetworkTimeoutMillis >= 0;
    }


    long getDeadlineNanos() {
        return deadlineNanos;
    }


    long remainingMillis() throws PrismInterfaceServiceException {
        return remainingMillis( deadlineNanos, timeoutMillis );
    }


    long remainingServerQueryMillis() throws PrismInterfaceServiceException {
        return remainingMillis( startedAtNanos + TimeUnit.MILLISECONDS.toNanos( serverQueryTimeoutMillis ), serverQueryTimeoutMillis );
    }


    boolean queryExpiresBeforeNetwork() {
        return serverQueryNetworkTimeoutMillis == 0 || serverQueryTimeoutMillis < serverQueryNetworkTimeoutMillis;
    }


    private static long remainingMillis( long deadlineNanos, long timeoutMillis ) throws PrismInterfaceServiceException {
        if ( timeoutMillis == 0 ) {
            return 0;
        }
        long remaining = deadlineNanos - System.nanoTime();
        if ( remaining <= 0 ) {
            throw new PrismInterfaceServiceException( new BeforeSendTimeout( timeoutMillis ) );
        }
        return 1 + TimeUnit.NANOSECONDS.toMillis( remaining - 1 );
    }


    <T> CallbackQueue<T> callback( Function<Response, T> extractResponse ) {
        long responseTimeoutMillis = usesServerQueryTimeout() ? serverQueryNetworkTimeoutMillis : timeoutMillis;
        long responseDeadline = startedAtNanos + TimeUnit.MILLISECONDS.toNanos( responseTimeoutMillis );
        return new CallbackQueue<>( extractResponse, responseTimeoutMillis, responseDeadline );
    }


    long getTransactionGeneration() {
        return transactionGeneration;
    }


    void recordTransactionGeneration( CallbackQueue<?> callback ) {
        transactionGeneration = callback.getTransactionGeneration();
    }


    static final class BeforeSendTimeout extends TimeoutException {

        BeforeSendTimeout( long timeoutMillis ) {
            super( "Prism request was not sent within " + timeoutMillis + " ms." );
        }

    }


    static final class QuerySendTimeout extends TimeoutException {

        QuerySendTimeout( long timeoutMillis ) {
            super( "Prism request send did not complete within " + timeoutMillis + " ms." );
        }

    }

}

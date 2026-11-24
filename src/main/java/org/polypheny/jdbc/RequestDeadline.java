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
    private final long deadlineNanos;


    RequestDeadline( long timeoutMillis ) {
        this.timeoutMillis = timeoutMillis;
        this.deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos( timeoutMillis );
    }


    long remainingMillis() throws PrismInterfaceServiceException {
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
        return new CallbackQueue<>( extractResponse, timeoutMillis, deadlineNanos );
    }


    static final class BeforeSendTimeout extends TimeoutException {

        BeforeSendTimeout( long timeoutMillis ) {
            super( "Prism request was not sent within " + timeoutMillis + " ms." );
        }

    }

}

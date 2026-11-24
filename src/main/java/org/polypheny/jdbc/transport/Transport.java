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

package org.polypheny.jdbc.transport;

import java.io.IOException;
import java.util.function.BooleanSupplier;

public interface Transport {

    void sendMessage( byte[] message ) throws IOException;

    /**
     * Attempts a timed send. Call beginWrite after preparing the message and acquiring any write lock.
     * A false return indicates that no bytes were sent.
     */
    default boolean trySendMessage( byte[] message, long deadlineNanos, BooleanSupplier beginWrite ) throws IOException {
        if ( !beginWrite.getAsBoolean() ) {
            return false;
        }
        sendMessage( message );
        return true;
    }

    /**
     * Keeps other sends from interleaving with a multi-request operation. The reader remains independent.
     * A zero timeout waits indefinitely. A false return means the operation was not started.
     */
    default <E extends Exception> boolean withWriteReservation( int timeoutMillis, WriteOperation<E> operation ) throws E, InterruptedException {
        operation.run();
        return true;
    }

    @FunctionalInterface
    interface WriteOperation<E extends Exception> {

        void run() throws E;

    }

    byte[] receiveMessage() throws IOException;

    void close();

}

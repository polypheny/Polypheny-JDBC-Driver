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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.prism.Response;
import org.polypheny.prism.StatementResponse;

class CallbackQueueTest {

    @Test
    void surfacesAnErrorDeliveredBeforeTheCallerStartsWaiting() {
        CallbackQueue<StatementResponse> queue = new CallbackQueue<>( Response::getStatementResponse );
        PrismInterfaceServiceException error = new PrismInterfaceServiceException( "Statement failed" );
        queue.onError( error );

        assertTimeoutPreemptively( Duration.ofSeconds( 2 ), () -> {
            assertSame( error, assertThrows( PrismInterfaceServiceException.class, queue::takeNext ) );
            assertSame( error, assertThrows( PrismInterfaceServiceException.class, queue::awaitSuccessfulCompletion ) );
        } );
    }


    @Test
    void rejectsTerminalFrameReceivedAfterTheDeadlineBeforeConsumption() {
        AtomicLong clock = new AtomicLong();
        CallbackQueue<Response> queue = new CallbackQueue<>( response -> response, 1, clock::get );
        clock.set( queue.getDeadlineNanos() + 1 );
        queue.onNext( Response.newBuilder().setLast( true ).build() );
        queue.onCompleted();

        assertTimeoutFailure( queue );
    }


    @Test
    void acceptsTerminalFrameQueuedBeforeTheDeadlineAndConsumedLater() throws Exception {
        AtomicLong clock = new AtomicLong();
        CallbackQueue<Response> queue = new CallbackQueue<>( response -> response, 1, clock::get );
        Response terminal = Response.newBuilder().setLast( true ).build();
        clock.set( queue.getDeadlineNanos() - 1 );
        queue.onNext( terminal );
        clock.set( queue.getDeadlineNanos() + 1 );
        queue.onCompleted();

        assertSame( terminal, queue.takeNext() );
        queue.awaitSuccessfulCompletion();
    }


    @Test
    void keepsAnOnTimeFrameButRejectsAnErrorReceivedAfterTheDeadline() throws Exception {
        AtomicLong clock = new AtomicLong();
        CallbackQueue<Response> queue = new CallbackQueue<>( response -> response, 1, clock::get );
        Response partial = Response.getDefaultInstance();
        queue.onNext( partial );
        clock.set( queue.getDeadlineNanos() );
        queue.onError( new PrismInterfaceServiceException( "Late server error" ) );

        assertSame( partial, queue.takeNext() );
        assertTimeoutFailure( queue );
    }


    @Test
    void rejectsACompletionCallbackReceivedAfterTheDeadline() {
        AtomicLong clock = new AtomicLong();
        CallbackQueue<Response> queue = new CallbackQueue<>( response -> response, 1, clock::get );
        clock.set( queue.getDeadlineNanos() );
        queue.onCompleted();

        assertTimeoutFailure( queue );
    }


    private static void assertTimeoutFailure( CallbackQueue<Response> queue ) {
        PrismInterfaceServiceException failure = assertThrows( PrismInterfaceServiceException.class, queue::takeNext );
        assertInstanceOf( TimeoutException.class, failure.getCause() );
        assertSame( failure, assertThrows( PrismInterfaceServiceException.class, queue::awaitSuccessfulCompletion ) );
    }

}

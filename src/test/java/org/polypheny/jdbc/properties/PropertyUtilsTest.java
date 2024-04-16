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

package org.polypheny.jdbc.properties;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import org.junit.jupiter.api.Test;

public class PropertyUtilsTest {

    @Test
    public void testDefaultTransactionIsolation() {
        final int expected = Connection.TRANSACTION_READ_COMMITTED;
        assertEquals( expected, PropertyUtils.getDEFAULT_TRANSACTION_ISOLATION() );
    }


    @Test
    public void testDefaultNetworkTimeout() {
        final int expected = 0;
        assertEquals( expected, PropertyUtils.getDEFAULT_NETWORK_TIMEOUT() );
    }


    @Test
    public void testDefaultQueryTimeoutSeconds() {
        final int expected = 0;
        assertEquals( expected, PropertyUtils.getDEFAULT_QUERY_TIMEOUT_SECONDS() );
    }


    @Test
    public void testDefaultFetchSize() {
        final int expected = 100;
        assertEquals( expected, PropertyUtils.getDEFAULT_FETCH_SIZE() );
    }


    @Test
    public void testDefaultFetchDirection() {
        final int expected = ResultSet.FETCH_FORWARD;
        assertEquals( expected, PropertyUtils.getDEFAULT_FETCH_DIRECTION() );
    }


    @Test
    public void testDefaultResultSetType() {
        final int expected = ResultSet.TYPE_FORWARD_ONLY;
        assertEquals( expected, PropertyUtils.getDEFAULT_RESULTSET_TYPE() );
    }


    @Test
    public void testDefaultResultSetConcurrency() {
        final int expected = ResultSet.CONCUR_READ_ONLY;
        assertEquals( expected, PropertyUtils.getDEFAULT_RESULTSET_CONCURRENCY() );
    }


    @Test
    public void testDefaultMaxFieldSize() {
        final int expected = 0;
        assertEquals( expected, PropertyUtils.getDEFAULT_MAX_FIELD_SIZE() );
    }


    @Test
    public void testDefaultLargeMaxRows() {
        final long expected = 0L;
        assertEquals( expected, PropertyUtils.getDEFAULT_LARGE_MAX_ROWS() );
    }


    @Test
    public void testDefaultDoingEscapeProcessing() {
        final boolean expected = true;
        assertEquals( expected, PropertyUtils.isDEFAULT_DOING_ESCAPE_PROCESSING() );
    }


    @Test
    public void testDefaultStatementPoolable() {
        final boolean expected = false;
        assertEquals( expected, PropertyUtils.isDEFAULT_STATEMENT_POOLABLE() );
    }


    @Test
    public void testDefaultPreparedStatementPoolable() {
        final boolean expected = false;
        assertEquals( expected, PropertyUtils.isDEFAULT_PREPARED_STATEMENT_POOLABLE() );
    }


    @Test
    public void testDefaultCallableStatementPoolable() {
        final boolean expected = false;
        assertEquals( expected, PropertyUtils.isDEFAULT_CALLABLE_STATEMENT_POOLABLE() );
    }


    @Test
    public void testDefaultAutocommit() {
        final boolean expected = true;
        assertEquals( expected, PropertyUtils.isDEFAULT_AUTOCOMMIT() );
    }


    @Test
    public void testDefaultReadOnly() {
        final boolean expected = false;
        assertEquals( expected, PropertyUtils.isDEFAULT_READ_ONLY() );
    }


    @Test
    public void defaultHostSetToLocalHost() {
        final String expected = "localhost";
        assertEquals( expected, PropertyUtils.getDEFAULT_HOST() );

    }


    @Test
    public void defaultPortIsCorrect() {
        final int expected = 20590;
        assertEquals( expected, PropertyUtils.getDEFAULT_PORT() );
    }


    @Test
    public void testDefaultResultSetHoldability() {
        final int expected = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        assertEquals( expected, PropertyUtils.getDEFAULT_RESULTSET_HOLDABILITY() );
    }


    @Test
    public void testDefaultHost() {
        final String expected = "localhost";
        assertEquals( expected, PropertyUtils.getDEFAULT_HOST() );
    }


    @Test
    public void testDefaultPort() {
        final int expected = 20590;
        assertEquals( expected, PropertyUtils.getDEFAULT_PORT() );
    }


    @Test
    public void testSqlLanguageName() {
        final String expected = "sql";
        assertEquals( expected, PropertyUtils.getSQL_LANGUAGE_NAME() );
    }


    @Test
    public void testUsernameKey() {
        final String expected = "user";
        assertEquals( expected, PropertyUtils.getUSERNAME_KEY() );
    }


    @Test
    public void testPasswordKey() {
        final String expected = "password";
        assertEquals( expected, PropertyUtils.getPASSWORD_KEY() );
    }


    @Test
    public void testNamespaceKey() {
        final String expected = "namespace";
        assertEquals( expected, PropertyUtils.getNAMESPACE_KEY() );
    }


    @Test
    public void testAutocommitKey() {
        final String expected = "autocommit";
        assertEquals( expected, PropertyUtils.getAUTOCOMMIT_KEY() );
    }


    @Test
    public void testReadOnlyKey() {
        final String expected = "readonly";
        assertEquals( expected, PropertyUtils.getREAD_ONLY_KEY() );
    }


    @Test
    public void testResultSetHoldabilityKey() {
        final String expected = "holdability";
        assertEquals( expected, PropertyUtils.getRESULT_SET_HOLDABILITY_KEY() );
    }


    @Test
    public void testNetworkTimeoutKey() {
        final String expected = "nwtimeout";
        assertEquals( expected, PropertyUtils.getNETWORK_TIMEOUT_KEY() );
    }


    @Test
    public void testTransactionIsolationKey() {
        final String expected = "isolation";
        assertEquals( expected, PropertyUtils.getTRANSACTION_ISOLATION_KEY() );
    }


    @Test
    public void testTimezoneKey() {
        final String expected = "timezone";
        assertEquals( expected, PropertyUtils.getTIMEZONE_KEY() );
    }


    @Test
    public void testStrictModeKey() {
        final String expected = "strict";
        assertEquals( expected, PropertyUtils.getSTRICT_MODE_KEY() );
    }

}

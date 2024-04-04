/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.jdbc.types;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public interface Convertible {

    boolean isNull();


    PolyDocument asDocument() throws SQLException;

    PolyInterval asInterval() throws SQLException;

    String asString() throws SQLException;


    boolean asBoolean() throws SQLException;


    byte asByte() throws SQLException;


    short asShort() throws SQLException;


    int asInt() throws SQLException;


    long asLong() throws SQLException;


    float asFloat() throws SQLException;


    double asDouble() throws SQLException;


    BigDecimal asBigDecimal() throws SQLException;


    BigDecimal asBigDecimal( int scale ) throws SQLException;


    byte[] asBytes() throws SQLException;


    InputStream asAsciiStream() throws SQLException;


    InputStream asUnicodeStream() throws SQLException;


    InputStream asBinaryStream() throws SQLException;


    Object asObject() throws SQLException;


    Reader asCharacterStream() throws SQLException;


    Blob asBlob() throws SQLException;


    Clob asClob() throws SQLException;


    Array asArray() throws SQLException;


    Struct asStruct() throws SQLException;

    Date asDate() throws SQLException;

    Date asDate( Calendar calendar ) throws SQLException;

    Time asTime() throws SQLException;

    Time asTime( Calendar calendar ) throws SQLException;

    Timestamp asTimestamp() throws SQLException;

    Timestamp asTimestamp( Calendar calendar ) throws SQLException;

    Ref asRef() throws SQLException;

    RowId asRowId() throws SQLException;

    URL asUrl() throws SQLException;


    NClob asNClob() throws SQLException;


    SQLXML asSQLXML() throws SQLException;


    String asNString() throws SQLException;


    Reader asNCharacterStream() throws SQLException;

    Object asObject( Map<String, Class<?>> map ) throws SQLException;

    Object asObject( Calendar calendar ) throws SQLException;

    <T> T asObject( Class<T> aClass ) throws SQLException;

}

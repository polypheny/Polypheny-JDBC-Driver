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

package org.polypheny.jdbc.types;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.ProtoValue.ValueCase;

public class TypedValueTest {


    @Test
    public void fromNCharacterStreamWithReader() throws IOException, SQLException {
        String string = "test";
        Reader reader = new StringReader( string );
        TypedValue typedValue = TypedValue.fromNCharacterStream( reader );
        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( string, typedValue.asString() );
    }


    @Test
    public void fromNStringWithValidString() throws SQLException {
        String value = "Hello World";
        TypedValue typedValue = TypedValue.fromNString( value );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( value, typedValue.asString() );
    }


    @Test
    public void fromNStringWithNullString() {
        String value = null;
        TypedValue typedValue = TypedValue.fromNString( value );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
    }


    @Test
    public void fromRowIdWithNullRowId() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> TypedValue.fromRowId( null ) );
    }


    @Test
    public void fromRowIdWithValidRowId() {
        RowId rowId = Mockito.mock( RowId.class );
        assertThrows( SQLFeatureNotSupportedException.class, () -> TypedValue.fromRowId( rowId ) );
    }


    @Test
    public void fromUrlWithNullUrl() {
        assertThrows( SQLFeatureNotSupportedException.class, () -> TypedValue.fromUrl( null ) );
    }


    @Test
    public void fromUrlWithValidUrl() throws MalformedURLException {
        URL url = new URL( "https://example.com" );
        assertThrows( SQLFeatureNotSupportedException.class, () -> TypedValue.fromUrl( url ) );
    }


    @Test
    public void fromArrayWithGivenValue() throws SQLException {
        Array value = Mockito.mock( Array.class );
        TypedValue typedValue = TypedValue.fromArray( value );

        assertEquals( ValueCase.LIST, typedValue.getValueCase() );
        assertEquals( value, typedValue.asArray() );
    }


    @Test
    public void fromClobWithClobValue() throws SQLException {
        String content = "This is awesome!";
        Clob clob = new PolyphenyClob( content );
        TypedValue typedValue = TypedValue.fromClob( clob );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( content, typedValue.asString() );
    }


    @Test
    public void fromNull() {
        TypedValue typedValue = TypedValue.fromNull();
        assertNotNull( typedValue );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
        assertTrue( typedValue.isNull() );
    }


    @Test
    public void fromBlobWithBlobValue() throws SQLException {
        byte[] data = { 2, 34, 5, 7 };
        Blob blob = new PolyphenyBlob( data );
        TypedValue typedValue = TypedValue.fromBlob( blob );
        assertEquals( ValueCase.FILE, typedValue.getValueCase() );
        assertEquals( blob, typedValue.asBlob() );
    }


    @Test
    public void fromBlobWithBlobValueAsByteThrows() throws SQLException {
        byte[] data = { 2, 34, 5, 7 };
        Blob blob = new PolyphenyBlob( data );
        TypedValue typedValue = TypedValue.fromBlob( blob );
        assertEquals( ValueCase.FILE, typedValue.getValueCase() );
        assertThrows( PrismInterfaceServiceException.class, typedValue::asBytes );
    }


    @Test
    public void fromCharacterStreamWithLength1() throws SQLException {
        Reader reader = new StringReader( "Hello World" );
        int length = 11;

        TypedValue typedValue = TypedValue.fromCharacterStream( reader, length );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "Hello World", typedValue.asString() );
    }


    @Test
    public void fromCharacterStreamWithLength2() throws SQLException {
        Reader reader = new StringReader( "Hello World" );
        long length = 11;

        TypedValue typedValue = TypedValue.fromCharacterStream( reader, length );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "Hello World", typedValue.asString() );
    }


    @Test
    public void fromCharacterStreamWithValidStream() throws SQLException {
        Reader reader = new StringReader( "Hello World" );

        TypedValue typedValue = TypedValue.fromCharacterStream( reader, 11 );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "Hello World", typedValue.asString() );
    }


    @Test
    public void fromCharacterStreamThrowsSQLException() throws SQLException {
        String inputString = "Test";
        InputStream inputStream = new ByteArrayInputStream( inputString.getBytes( StandardCharsets.UTF_8 ) );
        Reader reader = new InputStreamReader( inputStream );
        TypedValue typedValue = TypedValue.fromCharacterStream( reader );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( inputString, typedValue.asString() );
    }


    @Test
    public void fromBinaryStreamWithValidStream2() throws SQLException {
        InputStream stream = new ByteArrayInputStream( new byte[]{ 1, 2, 3, 4, 5 } );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream );

        assertNotNull( typedValue );
        assertEquals( ValueCase.BINARY, typedValue.getValueCase() );
        assertArrayEquals( new byte[]{ 1, 2, 3, 4, 5 }, typedValue.asBytes() );
    }


    @Test
    public void fromBinaryStreamWithValidStream1() throws SQLException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream );

        assertNotNull( typedValue );
        assertEquals( ValueCase.BINARY, typedValue.getValueCase() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), typedValue.asBytes() );
    }


    @Test
    public void fromBinaryStreamWithValidStream3() throws SQLException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream, 11 );

        assertNotNull( typedValue );
        assertEquals( ValueCase.BINARY, typedValue.getValueCase() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), typedValue.asBytes() );
    }


    @Test
    public void fromBinaryStreamWithValidStream4() throws SQLException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream, 11L );

        assertNotNull( typedValue );
        assertEquals( ValueCase.BINARY, typedValue.getValueCase() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), typedValue.asBytes() );
    }


    @Test()
    public void fromBinaryStreamWhenStreamIsInvalidThenThrowIOException() {
        assertThrows( NullPointerException.class, () -> TypedValue.fromBinaryStream( null, 0 ) );
    }


    @Test
    public void fromUnicodeStreamWithValidStream() throws SQLException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromUnicodeStream( stream, 0 );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "Hello World", typedValue.asString() );
    }


    @Test()
    public void fromUnicodeStreamWithInvalidStreamThrowsIOException() {
        assertThrows( NullPointerException.class, () -> TypedValue.fromUnicodeStream( null, 0 ) );
    }


    @Test()
    public void fromAsciiStreamWithInvalidInputStream() {
        assertThrows( NullPointerException.class, () -> TypedValue.fromAsciiStream( null, 0 ) );
    }


    @Test
    public void fromAsciiStreamWithValidInputStreamAndLength() throws SQLException {
        String inputString = "Hello, World!";
        InputStream inputStream = new ByteArrayInputStream( inputString.getBytes( StandardCharsets.US_ASCII ) );

        TypedValue typedValue = TypedValue.fromAsciiStream( inputStream, inputString.length() );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( inputString, typedValue.asString() );
    }


    @Test
    public void fromAsciiStreamWithLength() throws SQLException {
        String input = "Hello, World!";
        InputStream inputStream = new ByteArrayInputStream( input.getBytes( StandardCharsets.US_ASCII ) );

        TypedValue typedValue = TypedValue.fromAsciiStream( inputStream, input.length() );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( input, typedValue.asString() );
    }


    @Test
    public void fromAsciiStreamWithValidStream() throws SQLException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.US_ASCII ) );
        TypedValue typedValue = TypedValue.fromAsciiStream( stream );

        assertNotNull( typedValue );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "Hello World", typedValue.asString() );
    }


    @Test()
    public void fromTimeWithNullCalendarThrowsException() {
        assertThrows( NullPointerException.class, () -> TypedValue.fromTime( new Time( 10, 30, 0 ), null ) );
    }


    @Test
    public void timeZoneTest() {
        Time input = new Time( 123456 );
        Time input2 = new Time( input.getTime() );
        assertEquals( input, input2 );
    }


    @Test
    public void stringTrimmingTest() throws SQLException {
        TypedValue value = TypedValue.fromString( "123456789" );
        TypedValue trimmed = value.getTrimmed( 4 );
        assertEquals( "1234", trimmed.asString() );
    }


    @Test
    public void binaryTrimmingTest() throws SQLException {
        byte[] data = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        byte[] expected = { 1, 2, 3, 4 };
        TypedValue value = TypedValue.fromBytes( data );
        TypedValue trimmed = value.getTrimmed( 4 );
        assertArrayEquals( expected, trimmed.asBytes() );
    }


    @Test
    public void asBytesReturnsProperValue() throws SQLException {
        byte[] data = { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        TypedValue value = TypedValue.fromBytes( data );
        assertArrayEquals( data, value.asBytes() );
    }


    @Test
    public void fromTimeWithValidTimeAndCalendar() throws SQLException {
        Time time = new Time( 12, 30, 0 );
        Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, 2022 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 1 );
        calendar.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        TypedValue typedValue = TypedValue.fromTime( time, calendar );

        assertNotNull( typedValue );
        assertEquals( ValueCase.TIME, typedValue.getValueCase() );
        assertEquals( time, typedValue.asTime() );
    }


    @Test()
    public void fromTimeWithNullTimeAndCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, 2022 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 1 );

        assertThrows( NullPointerException.class, () -> TypedValue.fromTime( null, calendar ) );
    }


    @Test
    public void fromTimeWithNullTimeValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromTime( null );

        assertNotNull( typedValue );
        assertTrue( typedValue.isNull() );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
        assertNull( typedValue.asTime() );
    }


    @Test
    public void fromTimeWithValidTimeValue() throws SQLException {
        Time time = new Time( 12, 30, 0 );
        TypedValue typedValue = TypedValue.fromTime( time );

        assertNotNull( typedValue );
        assertEquals( ValueCase.TIME, typedValue.getValueCase() );
        assertEquals( time, typedValue.asTime() );
    }


    @Test
    public void asTimeWithValidValue() throws SQLException {
        Time time = new Time( 12, 30, 0 );
        TypedValue typedValue = TypedValue.fromTime( time );
        assertEquals( time, typedValue.asTime() );
    }


    @Test()
    public void fromDateWhenNullCalendarProvidedThenThrowException() {
        assertThrows( NullPointerException.class, () -> TypedValue.fromDate( new Date( 2022, 1, 1 ), null ) );
    }


    @Test
    public void fromDateWhenValidDateAndCalendarProvided() throws SQLException {
        Date date = new Date( 2021, Calendar.JANUARY, 1 );
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2022, Calendar.JANUARY, 1 );
        calendar.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        TypedValue typedValue = TypedValue.fromDate( date, calendar );

        assertEquals( ValueCase.DATE, typedValue.getValueCase() );
        assertEquals( date, typedValue.asDate() );
    }


    @Test()
    public void fromDateWithNullDateAndCalendarProvided() {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2022, Calendar.JANUARY, 1 );

        assertThrows( NullPointerException.class, () -> TypedValue.fromDate( null, calendar ) );
    }


    @Test
    public void fromDateWithNullDate() throws SQLException {
        TypedValue typedValue = TypedValue.fromDate( null );

        assertNotNull( typedValue );
        assertTrue( typedValue.isNull() );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
        assertNull( typedValue.asDate() );
    }


    @Test
    public void fromDateWithValidDate() throws SQLException {
        Date date = Date.valueOf( "2022-01-01" );
        TypedValue typedValue = TypedValue.fromDate( date );
        assertEquals( ValueCase.DATE, typedValue.getValueCase() );
        assertEquals( date, typedValue.asDate() );
    }


    @Test
    public void fromObjectWithValidDate() throws SQLException {
        Date date = Date.valueOf( "2022-01-01" );
        TypedValue typedValue = TypedValue.fromObject( date );
        assertEquals( ValueCase.DATE, typedValue.getValueCase() );
        assertEquals( date, typedValue.asDate() );
    }


    @Test
    public void fromBytesWithByteArray() throws SQLException {
        byte[] bytes = { 1, 2, 3, 4, 5 };
        TypedValue typedValue = TypedValue.fromBytes( bytes );
        assertEquals( ValueCase.BINARY, typedValue.getValueCase() );
        assertEquals( bytes, typedValue.asBytes() );
    }


    @Test
    public void fromStringWithEmptyString() throws SQLException {
        TypedValue typedValue = TypedValue.fromString( "" );

        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "", typedValue.asString() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromStringWithValidString() throws SQLException {
        TypedValue typedValue = TypedValue.fromString( "12345" );
        assertEquals( ValueCase.STRING, typedValue.getValueCase() );
        assertEquals( "12345", typedValue.asString() );
    }


    @Test
    public void fromStringWithNullString() throws SQLException {
        TypedValue typedValue = TypedValue.fromString( null );

        assertTrue( typedValue.isNull() );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
        assertNull( typedValue.asString() );
    }


    @Test
    public void fromBigDecimalWithValidInput() throws SQLException {
        BigDecimal value = new BigDecimal( "10.5" );
        TypedValue typedValue = TypedValue.fromBigDecimal( value );

        assertEquals( ValueCase.BIG_DECIMAL, typedValue.getValueCase() );
        assertEquals( value, typedValue.asBigDecimal() );
    }


    @Test
    public void fromBigDecimalWithNullInput() throws SQLException {
        TypedValue typedValue = TypedValue.fromBigDecimal( null );
        assertTrue( typedValue.isNull() );
        assertEquals( ValueCase.NULL, typedValue.getValueCase() );
        assertNull( typedValue.asBigDecimal() );
    }


    @Test
    public void fromDoubleWithValidInput() throws SQLException {
        TypedValue typedValue = TypedValue.fromDouble( 3.14 );

        assertEquals( ValueCase.DOUBLE, typedValue.getValueCase() );
        assertEquals( 3.14, typedValue.asDouble() );
    }


    @Test
    public void fromDoubleWithNegativeInput() throws SQLException {
        TypedValue typedValue = TypedValue.fromDouble( -10.5 );
        assertEquals( ValueCase.DOUBLE, typedValue.getValueCase() );
        assertEquals( -10.5, typedValue.asDouble() );
    }


    @Test
    public void fromDoubleWithZeroInput() throws SQLException {
        TypedValue typedValue = TypedValue.fromDouble( 0.0 );

        assertEquals( ValueCase.DOUBLE, typedValue.getValueCase() );
        assertEquals( 0.0, typedValue.asDouble() );
    }


    @Test
    public void fromFloatWithValidFloatValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromFloat( 3.14f );

        assertEquals( ValueCase.FLOAT, typedValue.getValueCase() );
        assertEquals( 3.14f, typedValue.asFloat() );
    }


    @Test
    public void fromLongWithValidInput() throws SQLException {
        TypedValue typedValue = TypedValue.fromLong( 1234567890L );

        assertEquals( ValueCase.LONG, typedValue.getValueCase() );
        assertEquals( 1234567890L, typedValue.asLong() );
    }


    @Test
    public void fromIntWithValidInteger() throws SQLException {
        TypedValue typedValue = TypedValue.fromInteger( 10 );

        assertEquals( ValueCase.INTEGER, typedValue.getValueCase() );
        assertEquals( 10, typedValue.asInt() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromShortWithValidShortValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromShort( (short) 10 );

        assertEquals( ValueCase.INTEGER, typedValue.getValueCase() );
        assertEquals( (short) 10, typedValue.asShort() );
        assertFalse( typedValue.isNull() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromByteWithValidByteValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromByte( (byte) 10 );

        assertEquals( ValueCase.INTEGER, typedValue.getValueCase() );
        assertEquals( (byte) 10, typedValue.asByte() );
    }


    @Test
    public void fromBooleanWithFalseValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromBoolean( false );

        assertEquals( ValueCase.BOOLEAN, typedValue.getValueCase() );
        assertFalse( typedValue.asBoolean() );
    }


    @Test
    public void fromBooleanWithTrueValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromBoolean( true );

        assertEquals( ValueCase.BOOLEAN, typedValue.getValueCase() );
        assertTrue( typedValue.asBoolean() );
    }


    @Test
    public void fromBooleanWithValidBooleanValue() throws SQLException {
        TypedValue typedValue = TypedValue.fromBoolean( true );

        assertEquals( ValueCase.BOOLEAN, typedValue.getValueCase() );
        assertTrue( typedValue.asBoolean() );
    }


    @Test
    public void fromPolyIntervalMonths() throws SQLException {
        PolyInterval interval = new PolyInterval( 23, 0 );
        TypedValue value = TypedValue.fromInterval( interval );
        assertFalse( value.isNull() );
        assertEquals( ValueCase.INTERVAL, value.getValueCase() );
        assertEquals( interval, value.asInterval() );
    }


    @Test
    public void fromPolyIntervalMillis() throws SQLException {
        PolyInterval interval = new PolyInterval( 0, 23 );
        TypedValue value = TypedValue.fromInterval( interval );
        assertFalse( value.isNull() );
        assertEquals( ValueCase.INTERVAL, value.getValueCase() );
        assertEquals( interval, value.asInterval() );
    }


    @Test
    public void fromPolyIntervalNull() throws SQLException {
        TypedValue value = TypedValue.fromInterval( null );
        assertTrue( value.isNull() );
        assertEquals( ValueCase.NULL, value.getValueCase() );
        assertNull( value.asInterval() );
    }


    @Test
    public void fromPolyDocument() throws SQLException {
        PolyDocument document = new PolyDocument();
        document.put( "firstValue", TypedValue.fromBoolean( true ) );
        document.put( "secondValue", TypedValue.fromDouble( 12.345 ) );
        document.put( "thirdValue", TypedValue.fromInterval( new PolyInterval( 69, 0 ) ) );

        TypedValue value = TypedValue.fromDocument( document );
        assertFalse( value.isNull() );
        assertEquals( ValueCase.DOCUMENT, value.getValueCase() );
        assertEquals( document, value.asDocument() );
    }


    @Test
    public void fromPolyDocumentNull() throws SQLException {
        TypedValue value = TypedValue.fromDocument( null );
        assertTrue( value.isNull() );
        assertEquals( ValueCase.NULL, value.getValueCase() );
        assertNull( value.asDocument() );
    }


    @Test
    void booleanTest() throws SQLException {
        boolean value = true;
        TypedValue typedValue1 = TypedValue.fromBoolean( true );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.BOOLEAN, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asBoolean() );
    }


    @Test
    void integerTest() throws SQLException {
        int value = 1234;
        TypedValue typedValue1 = TypedValue.fromInteger( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.INTEGER, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asInt() );
    }


    @Test
    void longTest() throws SQLException {
        long value = 1234L;
        TypedValue typedValue1 = TypedValue.fromLong( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.LONG, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asLong() );
    }


    @Test
    void binaryTest() throws SQLException {
        byte[] value = new byte[]{ 1, 2, 3, 4 };
        TypedValue typedValue1 = TypedValue.fromBytes( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.BINARY, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertArrayEquals( value, typedValue2.asBytes() );
    }


    @Test
    void binaryTestAsObject() throws SQLException {
        byte[] value = new byte[]{ 1, 2, 3, 4 };
        TypedValue typedValue1 = TypedValue.fromBytes( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.BINARY, protoValue.getValueCase() );

        assertArrayEquals( value, (byte[]) new TypedValue( protoValue ).asObject() );
    }


    @Test
    void dateTest() throws SQLException {
        Date value = new Date( 49852800000L );
        TypedValue typedValue1 = TypedValue.fromDate( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.DATE, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asDate() );
    }


    @Test
    void doubleTest() throws SQLException {
        double value = 1.234;
        TypedValue typedValue1 = TypedValue.fromDouble( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.DOUBLE, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asDouble() );
    }


    @Test
    void floatTest() throws SQLException {
        float value = 1.234f;
        TypedValue typedValue1 = TypedValue.fromFloat( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.FLOAT, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asFloat() );
    }


    @Test
    void nullTest() throws SQLException {
        TypedValue typedValue1 = TypedValue.fromNull();
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.NULL, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertNull( typedValue2.asObject() );
    }


    @Test
    void stringTest() throws SQLException {
        String value = "a string";
        TypedValue typedValue1 = TypedValue.fromString( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.STRING, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asString() );
    }


    @Test
    void timeTest() throws SQLException {
        Time value = new Time( 234975L );
        TypedValue typedValue1 = TypedValue.fromTime( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.TIME, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        long millis = typedValue2.asTime().getTime();
        millis -= DriverProperties.getDEFAULT_TIMEZONE().getOffset( millis );
        assertEquals( 234975L, millis );
    }


    @Test
    void timestampTest() throws SQLException {
        Timestamp value = new Timestamp( 47285720L );
        TypedValue typedValue1 = TypedValue.fromTimestamp( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.TIMESTAMP, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        long millis = typedValue2.asTimestamp().getTime();
        millis -= DriverProperties.getDEFAULT_TIMEZONE().getOffset( millis );
        assertEquals( 47285720L, millis );
    }


    @Test
    void bigDecimalTest() throws SQLException {
        BigDecimal value = new BigDecimal( "3457980.32453" );
        TypedValue typedValue1 = TypedValue.fromBigDecimal( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.BIG_DECIMAL, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asBigDecimal() );
    }


    @Test
    void listTest() throws SQLException {
        ArrayList<TypedValue> values = new ArrayList<>();
        values.add( TypedValue.fromInteger( 1 ) );
        values.add( TypedValue.fromInteger( 2 ) );
        values.add( TypedValue.fromInteger( 3 ) );
        Array value = new PolyphenyArray( "INTEGER", values );

        TypedValue typedValue1 = TypedValue.fromArray( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.LIST, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertArrayEquals( (Object[]) value.getArray(), (Object[]) typedValue2.asArray().getArray() );
    }


    @Test
    void intervalTest() throws SQLException {
        PolyInterval value = new PolyInterval( 32, 0 );
        TypedValue typedValue1 = TypedValue.fromInterval( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.INTERVAL, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value, typedValue2.asInterval() );
    }


    @Test
    void intervalTestAsObject() throws SQLException {
        PolyInterval value = new PolyInterval( 32, 0 );
        TypedValue typedValue1 = TypedValue.fromInterval( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.INTERVAL, protoValue.getValueCase() );

        assertEquals( value, new TypedValue( protoValue ).asObject() );
    }


    @Test
    void documentTest() throws SQLException {
        PolyDocument value = new PolyDocument();
        value.put( "firstValue", TypedValue.fromBoolean( true ) );
        value.put( "secondValue", TypedValue.fromDouble( 12.345 ) );
        value.put( "thirdValue", TypedValue.fromInterval( new PolyInterval( 69, 0 ) ) );

        TypedValue typedValue1 = TypedValue.fromDocument( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.DOCUMENT, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value.get( "firstValue" ).asBoolean(), typedValue2.asDocument().get( "firstValue" ).asBoolean() );
        assertEquals( value.get( "secondValue" ).asDouble(), typedValue2.asDocument().get( "secondValue" ).asDouble() );
        assertEquals( value.get( "thirdValue" ).asInterval(), typedValue2.asDocument().get( "thirdValue" ).asInterval() );
    }


    @Test
    void documentTestAsObject() throws SQLException {
        PolyDocument value = new PolyDocument();
        value.put( "firstValue", TypedValue.fromBoolean( true ) );
        value.put( "secondValue", TypedValue.fromDouble( 12.345 ) );
        value.put( "thirdValue", TypedValue.fromInterval( new PolyInterval( 69, 0 ) ) );

        TypedValue typedValue1 = TypedValue.fromDocument( value );
        ProtoValue protoValue = typedValue1.serialize();

        PolyDocument document = (PolyDocument) new TypedValue( protoValue ).asObject();
        assertEquals( value.get( "firstValue" ).asBoolean(), document.get( "firstValue" ).asBoolean() );
        assertEquals( value.get( "secondValue" ).asDouble(), document.get( "secondValue" ).asDouble() );
        assertEquals( value.get( "thirdValue" ).asInterval(), document.get( "thirdValue" ).asInterval() );
    }


    @Test
    void fileTest() throws SQLException {
        Blob value = new PolyphenyBlob( new byte[]{ 1, 2, 3, 4, 5 } );
        TypedValue typedValue1 = TypedValue.fromBlob( value );
        ProtoValue protoValue = typedValue1.serialize();

        assertEquals( ValueCase.FILE, protoValue.getValueCase() );

        TypedValue typedValue2 = new TypedValue( protoValue );
        assertArrayEquals( value.getBytes( 1, 5 ), typedValue2.asBlob().getBytes( 1, 5 ) );
    }


    @Test
    void getLengthTest() throws SQLException {
        String value = "12345678";
        TypedValue typedValue1 = TypedValue.fromString( value );
        ProtoValue protoValue = typedValue1.serialize();
        TypedValue typedValue2 = new TypedValue( protoValue );
        assertEquals( value.length(), typedValue2.getLength() );
    }

}

package org.polypheny.jdbc.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import java.sql.Time;
import java.sql.Types;
import java.util.Calendar;
import java.util.TimeZone;
import org.junit.Test;
import org.mockito.Mockito;

public class TypedValueTest {


    @Test
    public void fromNCharacterStreamWithReader() throws IOException {
        String string = "test";
        Reader reader = new StringReader( string );
        TypedValue typedValue = TypedValue.fromNCharacterStream( reader );
        assertNotNull( typedValue );
        assertEquals( Types.NVARCHAR, typedValue.getJdbcType() );
        assertEquals( string, typedValue.getValue() );
    }


    @Test
    public void fromNStringWithValidString() {
        String value = "Hello World";
        TypedValue typedValue = TypedValue.fromNString( value );

        assertEquals( Types.NVARCHAR, typedValue.getJdbcType() );
        assertEquals( value, typedValue.getValue() );
    }


    @Test
    public void fromNStringWithNullString() {
        String value = null;
        TypedValue typedValue = TypedValue.fromNString( value );
        assertNull( typedValue.getValue() );
        assertEquals( Types.NVARCHAR, typedValue.getJdbcType() );
    }


    @Test
    public void fromRowIdWithNullRowId() {
        TypedValue typedValue = TypedValue.fromRowId( null );
        assertNull( typedValue.getValue() );
        assertEquals( Types.ROWID, typedValue.getJdbcType() );
    }


    @Test
    public void fromRowIdWithValidRowId() {
        RowId rowId = Mockito.mock( RowId.class );

        TypedValue typedValue = TypedValue.fromRowId( rowId );

        assertEquals( Types.ROWID, typedValue.getJdbcType() );
        assertEquals( rowId, typedValue.getValue() );
    }


    @Test
    public void fromUrlWithNullUrl() {
        TypedValue typedValue = TypedValue.fromUrl( null );
        assertTrue( typedValue.isNull() );
        assertFalse( typedValue.isSqlNull() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromUrlWithValidUrl() throws MalformedURLException {
        URL url = new URL( "https://example.com" );
        TypedValue typedValue = TypedValue.fromUrl( url );

        assertNotNull( typedValue );
        assertEquals( Types.DATALINK, typedValue.getJdbcType() );
        assertEquals( url, typedValue.getValue() );
    }


    @Test
    public void fromArrayWithGivenValue() {
        Array value = Mockito.mock( Array.class );
        TypedValue typedValue = TypedValue.fromArray( value );

        assertEquals( Types.ARRAY, typedValue.getJdbcType() );
        assertEquals( value, typedValue.getValue() );
    }


    @Test
    public void fromClobWithClobValue() {
        Clob clob = Mockito.mock( Clob.class );

        TypedValue typedValue = TypedValue.fromClob( clob );

        assertEquals( Types.CLOB, typedValue.getJdbcType() );

        assertEquals( clob, typedValue.getValue() );
    }


    @Test
    public void fromNullWhenSqlTypeNotInNullMap() {
        int sqlType = Types.INTEGER;
        TypedValue typedValue = TypedValue.fromNull( sqlType );

        assertNotNull( typedValue );
        assertEquals( sqlType, typedValue.getJdbcType() );
        assertNull( typedValue.getValue() );
        assertTrue( typedValue.isNull() );
        assertFalse( typedValue.isSqlNull() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromBlobWithBlobValue() {
        Blob blob = Mockito.mock( Blob.class );

        TypedValue typedValue = TypedValue.fromBlob( blob );

        assertEquals( Types.BLOB, typedValue.getJdbcType() );

        assertEquals( blob, typedValue.getValue() );
    }


    @Test
    public void fromCharacterStreamWithLength1() throws IOException {
        Reader reader = new StringReader( "Hello World" );
        int length = 11;

        TypedValue typedValue = TypedValue.fromCharacterStream( reader, length );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "Hello World", typedValue.getValue() );
    }


    @Test
    public void fromCharacterStreamWithLength2() throws IOException {
        Reader reader = new StringReader( "Hello World" );
        long length = 11;

        TypedValue typedValue = TypedValue.fromCharacterStream( reader, length );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "Hello World", typedValue.getValue() );
    }


    @Test
    public void fromCharacterStreamWithValidStream() throws IOException {
        Reader reader = new StringReader( "Hello World" );

        TypedValue result = TypedValue.fromCharacterStream( reader, 11 );

        assertNotNull( result );
        assertEquals( Types.VARCHAR, result.getJdbcType() );
        assertEquals( "Hello World", result.getValue() );
    }


    @Test
    public void fromCharacterStreamThrowsIOException() throws IOException {
        String inputString = "Test";
        InputStream inputStream = new ByteArrayInputStream( inputString.getBytes( StandardCharsets.UTF_8 ) );
        Reader reader = new InputStreamReader( inputStream );
        TypedValue typedValue = TypedValue.fromCharacterStream( reader );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( inputString, typedValue.getValue() );
    }


    @Test
    public void fromBinaryStreamWithValidStream2() throws IOException {
        InputStream stream = new ByteArrayInputStream( new byte[]{ 1, 2, 3, 4, 5 } );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream );

        assertNotNull( typedValue );
        assertEquals( Types.BINARY, typedValue.getJdbcType() );
        assertArrayEquals( new byte[]{ 1, 2, 3, 4, 5 }, (byte[]) typedValue.getValue() );
    }


    @Test
    public void fromBinaryStreamWithValidStream1() throws IOException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream );

        assertNotNull( typedValue );
        assertEquals( Types.BINARY, typedValue.getJdbcType() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), (byte[]) typedValue.getValue() );
    }


    @Test
    public void fromBinaryStreamWithValidStream3() throws IOException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream, 11 );

        assertNotNull( typedValue );
        assertEquals( Types.BINARY, typedValue.getJdbcType() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), (byte[]) typedValue.getValue() );
    }


    @Test
    public void fromBinaryStreamWithValidStream4() throws IOException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromBinaryStream( stream, 11L );

        assertNotNull( typedValue );
        assertEquals( Types.BINARY, typedValue.getJdbcType() );
        assertArrayEquals( "Hello World".getBytes( StandardCharsets.UTF_8 ), (byte[]) typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromBinaryStreamWhenStreamIsInvalidThenThrowIOException() throws IOException {
        TypedValue.fromBinaryStream( null, 0 );
        fail( "NullPointer exception not thrown" );
    }


    @Test
    public void fromUnicodeStreamWithValidStream() throws IOException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.UTF_8 ) );
        TypedValue typedValue = TypedValue.fromUnicodeStream( stream, 0 );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "Hello World", typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromUnicodeStreamWithInvalidStreamThrowsIOException() throws IOException {
        TypedValue typedValue = TypedValue.fromUnicodeStream( null, 0 );
        fail( "NullPointer exception not thrown" );
    }


    @Test(expected = NullPointerException.class)
    public void fromAsciiStreamWithInvalidInputStream() throws IOException {
        TypedValue typedValue = TypedValue.fromAsciiStream( null, 0 );
        assertNull( typedValue );
        fail( "NullPointer exception not thrown" );
    }


    @Test
    public void fromAsciiStreamWithValidInputStreamAndLength() throws IOException {
        String inputString = "Hello, World!";
        InputStream inputStream = new ByteArrayInputStream( inputString.getBytes( StandardCharsets.US_ASCII ) );

        TypedValue typedValue = TypedValue.fromAsciiStream( inputStream, inputString.length() );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( inputString, typedValue.getValue() );
    }


    @Test
    public void fromAsciiStreamWithLength() throws IOException {
        String input = "Hello, World!";
        InputStream inputStream = new ByteArrayInputStream( input.getBytes( StandardCharsets.US_ASCII ) );

        TypedValue typedValue = TypedValue.fromAsciiStream( inputStream, input.length() );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( input, typedValue.getValue() );
    }


    @Test
    public void fromAsciiStreamWithValidStream() throws IOException {
        InputStream stream = new ByteArrayInputStream( "Hello World".getBytes( StandardCharsets.US_ASCII ) );
        TypedValue typedValue = TypedValue.fromAsciiStream( stream );

        assertNotNull( typedValue );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "Hello World", typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromTimeWithNullCalendarThrowsException() {
        TypedValue typedValue = TypedValue.fromTime( new Time( 10, 30, 0 ), null );
        assertNull( typedValue );
        fail( "NullPointerException not thrown" );
    }


    @Test
    public void fromTimeWithValidTimeAndCalendar() {
        Time time = new Time( 12, 30, 0 );
        Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, 2022 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 1 );
        calendar.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        TypedValue typedValue = TypedValue.fromTime( time, calendar );

        assertNotNull( typedValue );
        assertEquals( Types.TIME, typedValue.getJdbcType() );
        assertEquals( time, typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromTimeWithNullTimeAndCalendar() {
        Calendar calendar = Calendar.getInstance();
        calendar.set( Calendar.YEAR, 2022 );
        calendar.set( Calendar.MONTH, Calendar.JANUARY );
        calendar.set( Calendar.DAY_OF_MONTH, 1 );

        TypedValue typedValue = TypedValue.fromTime( null, calendar );

        fail( "NullPointerException not thrown" );
    }


    @Test
    public void fromTimeWithNullTimeValue() {
        TypedValue typedValue = TypedValue.fromTime( null );

        assertNotNull( typedValue );
        assertTrue( typedValue.isNull() );
        assertFalse( typedValue.isSqlNull() );
        assertEquals( Types.TIME, typedValue.getJdbcType() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromTimeWithValidTimeValue() {
        Time time = new Time( 12, 30, 0 );
        TypedValue typedValue = TypedValue.fromTime( time );

        assertNotNull( typedValue );
        assertEquals( Types.TIME, typedValue.getJdbcType() );
        assertEquals( time, typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromDateWhenNullCalendarProvidedThenThrowException() {
        TypedValue typedValue = TypedValue.fromDate( new Date( 2022, 1, 1 ), null );
        assertNull( typedValue );
        fail( "NullPointerException not thrown" );
    }


    @Test
    public void fromDateWhenValidDateAndCalendarProvided() {
        Date date = new Date( 2021, Calendar.JANUARY, 1 );
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2022, Calendar.JANUARY, 1 );
        calendar.setTimeZone( TimeZone.getTimeZone( "UTC" ) );

        TypedValue typedValue = TypedValue.fromDate( date, calendar );

        assertEquals( Types.DATE, typedValue.getJdbcType() );
        assertEquals( date, typedValue.getValue() );
    }


    @Test(expected = NullPointerException.class)
    public void fromDateWithNullDateAndCalendarProvided() {
        Calendar calendar = Calendar.getInstance();
        calendar.set( 2022, Calendar.JANUARY, 1 );

        TypedValue typedValue = TypedValue.fromDate( null, calendar );

        fail( "NullPointerException not thrown" );
    }


    @Test
    public void fromDateWithNullDate() {
        TypedValue typedValue = TypedValue.fromDate( null );

        assertNotNull( typedValue );
        assertTrue( typedValue.isNull() );
        assertFalse( typedValue.isSqlNull() );
        assertEquals( Types.DATE, typedValue.getJdbcType() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromDateWithValidDate() {
        Date date = Date.valueOf( "2022-01-01" );

        TypedValue typedValue = TypedValue.fromDate( date );

        assertEquals( Types.DATE, typedValue.getJdbcType() );
        assertEquals( date, typedValue.getValue() );
    }


    @Test
    public void fromBytesWithByteArray() {
        byte[] bytes = { 1, 2, 3, 4, 5 };
        TypedValue typedValue = TypedValue.fromBytes( bytes );

        assertEquals( Types.BINARY, typedValue.getJdbcType() );
        assertEquals( bytes, typedValue.getValue() );
    }


    @Test
    public void fromStringWithEmptyString() {
        TypedValue typedValue = TypedValue.fromString( "" );

        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "", typedValue.getValue() );
        assertFalse( typedValue.isSqlNull() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromStringWithValidString() {
        TypedValue typedValue = TypedValue.fromString( "12345" );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertEquals( "12345", typedValue.getValue() );
    }


    @Test
    public void fromStringWithNullString() {
        TypedValue typedValue = TypedValue.fromString( null );

        assertTrue( typedValue.isNull() );
        assertFalse( typedValue.isSqlNull() );
        assertEquals( Types.VARCHAR, typedValue.getJdbcType() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromBigDecimalWithValidInput() {
        BigDecimal value = new BigDecimal( "10.5" );
        TypedValue typedValue = TypedValue.fromBigDecimal( value );

        assertEquals( Types.NUMERIC, typedValue.getJdbcType() );
        assertEquals( value, typedValue.getValue() );
    }


    @Test
    public void fromBigDecimalWithNullInput() {
        TypedValue typedValue = TypedValue.fromBigDecimal( null );
        assertFalse( typedValue.isSqlNull() );
        assertTrue( typedValue.isNull() );
        assertEquals( Types.NUMERIC, typedValue.getJdbcType() );
        assertNull( typedValue.getValue() );
    }


    @Test
    public void fromDoubleWithValidInput() {
        TypedValue typedValue = TypedValue.fromDouble( 3.14 );

        assertEquals( Types.DOUBLE, typedValue.getJdbcType() );
        assertEquals( 3.14, typedValue.getValue() );
    }


    @Test
    public void fromDoubleWithNegativeInput() {
        TypedValue typedValue = TypedValue.fromDouble( -10.5 );

        assertEquals( Types.DOUBLE, typedValue.getJdbcType() );
        assertEquals( -10.5, typedValue.getValue() );
    }


    @Test
    public void fromDoubleWithZeroInput() {
        TypedValue typedValue = TypedValue.fromDouble( 0.0 );

        assertEquals( Types.DOUBLE, typedValue.getJdbcType() );
        assertEquals( 0.0, typedValue.getValue() );
    }


    @Test
    public void fromFloatWithValidFloatValue() {
        TypedValue typedValue = TypedValue.fromFloat( 3.14f );

        assertEquals( Types.REAL, typedValue.getJdbcType() );
        assertEquals( 3.14f, typedValue.getValue() );
    }


    @Test
    public void fromLongWithValidInput() {
        TypedValue typedValue = TypedValue.fromLong( 1234567890L );

        assertEquals( Types.BIGINT, typedValue.getJdbcType() );
        assertEquals( 1234567890L, typedValue.getValue() );
    }


    @Test
    public void fromIntWithValidInteger() {
        TypedValue typedValue = TypedValue.fromInt( 10 );

        assertEquals( Types.INTEGER, typedValue.getJdbcType() );
        assertEquals( 10, typedValue.getValue() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromShortWithValidShortValue() {
        TypedValue typedValue = TypedValue.fromShort( (short) 10 );

        assertEquals( Types.SMALLINT, typedValue.getJdbcType() );
        assertEquals( (short) 10, typedValue.getValue() );
        assertFalse( typedValue.isSqlNull() );
        assertFalse( typedValue.isNull() );
    }


    @Test
    public void fromByteWithValidByteValue() {
        TypedValue typedValue = TypedValue.fromByte( (byte) 10 );

        assertEquals( Types.TINYINT, typedValue.getJdbcType() );
        assertEquals( (byte) 10, typedValue.getValue() );
    }


    @Test
    public void fromBooleanWithFalseValue() {
        TypedValue typedValue = TypedValue.fromBoolean( false );

        assertEquals( Types.BOOLEAN, typedValue.getJdbcType() );
        assertFalse( (boolean) typedValue.getValue() );
    }


    @Test
    public void fromBooleanWithTrueValue() {
        TypedValue typedValue = TypedValue.fromBoolean( true );

        assertEquals( Types.BOOLEAN, typedValue.getJdbcType() );
        assertTrue( (Boolean) typedValue.getValue() );
    }


    @Test
    public void fromBooleanWithValidBooleanValue() {
        TypedValue typedValue = TypedValue.fromBoolean( true );

        assertEquals( Types.BOOLEAN, typedValue.getJdbcType() );
        assertEquals( true, typedValue.getValue() );
    }

}
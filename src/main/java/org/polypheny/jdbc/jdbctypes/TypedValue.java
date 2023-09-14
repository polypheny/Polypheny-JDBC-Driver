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

package org.polypheny.jdbc.jdbctypes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLInput;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.deserialization.UDTPrototype;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class TypedValue implements Convertible {

    private static final String UDT_PROOTYPE_TYPE = "UDT_PROTOTYPE";

    @Getter
    private final int jdbcType;
    @Getter
    private Object value;
    @Getter
    private String internalType;
    private static final HashMap<Integer, TypedValue> NULL_MAP = new HashMap<>();


    private TypedValue( int jdbcType, Object value ) {
        this.jdbcType = jdbcType;
        this.value = value;
        this.internalType = null;
    }


    private TypedValue( UDTPrototype udtPrototype ) {
        this.jdbcType = Types.OTHER;
        this.value = udtPrototype;
        this.internalType = UDT_PROOTYPE_TYPE;
    }


    public static TypedValue fromUdtPrototype( UDTPrototype udtPrototype ) {
        return new TypedValue( udtPrototype );
    }


    public static TypedValue fromBoolean( boolean value ) {
        return new TypedValue( Types.BOOLEAN, value );
    }


    public static TypedValue fromByte( byte value ) {
        return new TypedValue( Types.TINYINT, value );
    }


    public static TypedValue fromShort( short value ) {
        return new TypedValue( Types.SMALLINT, value );
    }


    public static TypedValue fromInt( int value ) {
        return new TypedValue( Types.INTEGER, value );
    }


    public static TypedValue fromLong( long value ) {
        return new TypedValue( Types.BIGINT, value );
    }


    public static TypedValue fromFloat( float value ) {
        return new TypedValue( Types.REAL, value );
    }


    public static TypedValue fromDouble( double value ) {
        return new TypedValue( Types.DOUBLE, value );
    }


    public static TypedValue fromBigDecimal( BigDecimal value ) {
        return new TypedValue( Types.NUMERIC, value );
    }


    public static TypedValue fromString( String value ) {
        /* differentiation between VARCHAR and LONGVARCHAR can be ignored as value is converted to PolyString anyway */
        return new TypedValue( Types.VARCHAR, value );
    }


    public static TypedValue fromBytes( byte[] bytes ) {
        /* differentiation between VARBINARY and LONGVARBINARY can be ignored as value is converted to PolyBinary anyway */
        return new TypedValue( Types.BINARY, bytes );
    }


    public static TypedValue fromDate( Date value ) {
        return new TypedValue( Types.DATE, value );
    }


    public static TypedValue fromDate( Date value, Calendar calendar ) {
        if ( calendar == null ) {
            throw new NullPointerException( "Calendar must not be null" );
        }
        return fromDate( TypedValueUtils.getDateInCalendar( value, calendar ) );
    }


    public static TypedValue fromTime( Time value ) {
        return new TypedValue( Types.TIME, value );
    }


    public static TypedValue fromTime( Time value, Calendar calendar ) {
        if ( calendar == null ) {
            throw new NullPointerException( "Calendar must not be null" );
        }
        return fromTime( TypedValueUtils.getTimeInCalendar( value, calendar ) );
    }


    public static TypedValue fromTimestamp( Timestamp value ) {
        return new TypedValue( Types.TIMESTAMP, value );
    }


    public static TypedValue fromTimestamp( Timestamp value, Calendar calendar ) throws NotImplementedException {
        if ( calendar == null ) {
            throw new NullPointerException( "Calendar must not be null" );
        }
        return fromTimestamp( TypedValueUtils.getTimestampInCalendar( value, calendar ) );
    }


    public static TypedValue fromAsciiStream( InputStream stream, int length ) throws IOException {
        return fromAsciiStream( stream );
    }


    public static TypedValue fromAsciiStream( InputStream stream, long length ) throws IOException {
        return fromAsciiStream( stream );
    }


    public static TypedValue fromAsciiStream( InputStream stream ) throws IOException {
        byte[] bytes = collectByteStream( stream );
        return fromString( new String( bytes, StandardCharsets.US_ASCII ) );
    }


    public static TypedValue fromUnicodeStream( InputStream stream, int length ) throws IOException {
        byte[] bytes = collectByteStream( stream );
        return fromString( new String( bytes, StandardCharsets.UTF_8 ) );
    }


    public static TypedValue fromBinaryStream( InputStream stream ) throws IOException {
        byte[] bytes = collectByteStream( stream );
        return fromBytes( bytes );
    }


    public static TypedValue fromBinaryStream( InputStream stream, int length ) throws IOException {
        return fromBinaryStream( stream );
    }


    public static TypedValue fromBinaryStream( InputStream stream, long length ) throws IOException {
        return fromBinaryStream( stream );
    }


    public static TypedValue fromCharacterStream( Reader reader ) throws IOException {
        return TypedValue.fromString( collectCharacterStream( reader ) );
    }


    public static TypedValue fromCharacterStream( Reader reader, int length ) throws IOException {
        return fromCharacterStream( reader );
    }


    public static TypedValue fromCharacterStream( Reader reader, long length ) throws IOException {
        return fromCharacterStream( reader );
    }


    public static TypedValue fromRef( Ref value ) {
        return new TypedValue( Types.REF, value );
    }


    public static TypedValue fromBlob( Blob value ) {
        return new TypedValue( Types.BLOB, value );
    }


    public static TypedValue fromBlob( InputStream stream ) throws IOException, NotImplementedException {
        byte[] bates = collectByteStream( stream );
        // TODO build BLOB from bytes...
        throw new NotImplementedException( "Not implemented yet..." );
    }


    public static TypedValue fromNull( int sqlType ) {
        if ( NULL_MAP.containsKey( sqlType ) ) {
            return NULL_MAP.get( sqlType );
        }
        TypedValue nullValue = new TypedValue( sqlType, null );
        NULL_MAP.put( sqlType, nullValue );
        return nullValue;
    }

    public static TypedValue fromNull(int sqlType, String internalType) {
        TypedValue typedValue = TypedValue.fromNull(sqlType);
        typedValue.internalType = internalType;
        return typedValue;
    }


    public static TypedValue fromNull() {
        return fromNull( Types.NULL );
    }

    public static TypedValue fromClob( Clob value ) {
        return new TypedValue( Types.CLOB, value );
    }


    public static TypedValue fromClob( Reader reader ) throws NotImplementedException {
        //TODO TH build CLOB from string...
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromClob( Reader reader, long length ) throws NotImplementedException {
        //TODO TH build CLOB from string...
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromArray( Array value ) {
        return new TypedValue( Types.ARRAY, value );
    }


    public static TypedValue fromUrl( URL value ) {
        return new TypedValue( Types.DATALINK, value );
    }


    public static TypedValue fromRowId( RowId value ) {
        return new TypedValue( Types.ROWID, value );
    }


    public static TypedValue fromNString( String value ) {
        /* differentiation between NVARCHAR and LONGNVARCHAR can be ignored as value is converted to PolyString anyway */
        return new TypedValue( Types.NVARCHAR, value );
    }


    public static TypedValue fromNCharacterStream( Reader reader ) throws IOException {
        /* differentiation between NVARCHAR and LONGNVARCHAR can be ignored as value is converted to PolyString anyway */
        return new TypedValue( Types.NVARCHAR, collectNCharacterStream( reader ) );
    }


    public static TypedValue fromNCharacterStream( Reader reader, long length ) throws IOException {
        return fromNCharacterStream( reader );
    }


    public static TypedValue fromNClob( NClob value ) {
        return new TypedValue( Types.NCLOB, value );
    }


    public static TypedValue fromNClob( Reader reader ) throws NotImplementedException {
        //TODO TH build NCLOB from string...
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromNClob( Reader reader, long length ) throws NotImplementedException {
        return fromNClob( reader );
    }


    public static TypedValue fromBlob( InputStream stream, long length ) throws NotImplementedException {
        //TODO TH build BLOB from bytes...
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromSQLXML( SQLXML value ) {
        return new TypedValue( Types.SQLXML, value );
    }


    public static TypedValue fromStruct( Struct value ) {
        return new TypedValue( Types.STRUCT, value );
    }


    private static String collectCharacterStream( Reader reader ) throws IOException {
        char[] readBuffer = new char[8 * 1024];
        StringBuilder buffer = new StringBuilder();
        int bufferIndex;
        while ( (bufferIndex = reader.read( readBuffer, 0, readBuffer.length )) != -1 ) {
            buffer.append( readBuffer, 0, bufferIndex );
        }
        reader.close();
        return buffer.toString();
    }


    private static byte[] collectByteStream( InputStream stream ) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int frameLength;
        byte[] frame = new byte[4];
        while ( (frameLength = stream.read( frame, 0, frame.length )) != -1 ) {
            buffer.write( frame, 0, frameLength );
        }
        buffer.flush();
        return buffer.toByteArray();
    }


    private static String collectNCharacterStream( Reader reader ) throws IOException {
        return collectCharacterStream( reader );
    }


    public static TypedValue fromJavaObject( Object value ) {
        return new TypedValue( Types.JAVA_OBJECT, value );
    }


    public int getLength() throws SQLException {
        switch ( jdbcType ) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return ((byte[]) value).length;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                return ((String) value).length();
        }
        return 0;
    }


    @Override
    public boolean isSqlNull() {
        return jdbcType == Types.NULL;
    }


    @Override
    public boolean isNull() {
        return value == null;
    }


    @Override
    public String asString() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        return value.toString();
    }


    @Override
    public boolean asBoolean() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            // jdbc4: if the value is SQL NULL, the value returned is false
            return false;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return TypedValueUtils.getBooleanFromNumber( (Number) value );
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return (Boolean) value;
        }
        if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
            return TypedValueUtils.getBooleanFromString( (String) value );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion to BOOLEAN is not supported." );
    }


    @Override
    public byte asByte() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).byteValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).byteValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Byte.parseByte( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to byte" );
    }


    @Override
    public short asShort() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).shortValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).shortValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Short.parseShort( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to short" );
    }


    @Override
    public int asInt() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).intValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).intValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Integer.parseInt( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to int" );
    }


    @Override
    public long asLong() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).longValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).longValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Long.parseLong( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to long" );
    }


    @Override
    public float asFloat() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).floatValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).floatValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Float.parseFloat( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to float" );
    }


    @Override
    public double asDouble() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return 0;
        }
        if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
            return ((Number) value).doubleValue();
        }
        if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
            return TypedValueUtils.getNumberFromBoolean( (Boolean) value ).doubleValue();
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return Double.parseDouble( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to double" );
    }


    @Override
    public BigDecimal asBigDecimal( int scale ) throws SQLException {
        return asBigDecimal().setScale( scale, RoundingMode.HALF_EVEN );
    }


    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        try {
            if ( TypedValueUtils.isNumberRepresented( jdbcType ) ) {
                if ( value instanceof BigDecimal ) {
                    return (BigDecimal) value;
                }
                return new BigDecimal( value.toString() );
            }
            if ( TypedValueUtils.isBooleanRepresented( jdbcType ) ) {
                return TypedValueUtils.getBigDecimalFromBoolean( (Boolean) value );
            }
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return new BigDecimal( (String) value );
            }
        } catch ( NumberFormatException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to byte" );
    }


    @Override
    public byte[] asBytes() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof byte[]) {
            return (byte[])  value;
        }
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream( byteArrayOutputStream );
            objectOutputStream.writeObject( value );
            return byteArrayOutputStream.toByteArray();
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error in converting object to byte array.", e );
        }
    }


    @Override
    public InputStream asAsciiStream() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof String ) {
            return new ByteArrayInputStream( ((String) value).getBytes( StandardCharsets.US_ASCII ) );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion to ascii stream not supported." );
    }


    private byte[] encodeString( String string, Charset charset ) throws SQLException {
        // separate method for error handling
        return string.getBytes( charset );
    }


    @Override
    public InputStream asUnicodeStream() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof String ) {
            return new ByteArrayInputStream( ((String) value).getBytes( StandardCharsets.UTF_8 ) );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion to unicode stream not supported." );
    }


    @Override
    public InputStream asBinaryStream() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof byte[] ) {
            return new ByteArrayInputStream( (byte[]) value );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion to binary stream not supported." );
    }


    @Override
    public Reader asCharacterStream() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof String ) {
            return new StringReader( (String) value );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a character stream" );

    }


    @Override
    public Blob asBlob() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof Blob ) {
            return (Blob) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a blob" );
    }


    @Override
    public Clob asClob() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( TypedValueUtils.isClobOrNClobRepresented( jdbcType ) ) {
            return (Clob) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a clob" );
    }


    @Override
    public Array asArray() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof Array ) {
            return (Array) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to an array" );
    }


    @Override
    public Struct asStruct() throws SQLException {
        if ( value instanceof Struct ) {
            return (Struct) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a struct" );
    }


    @Override
    public Date asDate() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        switch ( jdbcType ) {
            case Types.DATE:
                return (Date) value;
            case Types.TIMESTAMP:
                return  TypedValueUtils.getDateFromTimestamp( (Timestamp) value );
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return TypedValueUtils.getDateFromString( (String) value );
            }
        } catch ( ParseException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a date" );
    }


    @Override
    public Date asDate( Calendar calendar ) throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        Date date = asDate();
        return TypedValueUtils.getDateInCalendar( date, calendar );

    }


    public Time asTime() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        switch ( jdbcType ) {
            case Types.TIME:
                return (Time) value;
            case Types.DATE:
                return TypedValueUtils.getTimeFromDate( (Date) value );
            case Types.TIMESTAMP:
                return TypedValueUtils.getTimeFromTimestamp( (Timestamp) value );
        }
        try {
            if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
                return TypedValueUtils.getTimeFromString( (String) value );
            }
        } catch ( ParseException ignored ) {
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to time " + value + "with type " + jdbcType );
    }


    @Override
    public Time asTime( Calendar calendar ) throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        return TypedValueUtils.getTimeInCalendar( asTime(), calendar );
    }


    public Timestamp asTimestamp() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        switch ( jdbcType ) {
            case Types.TIMESTAMP:
                return (Timestamp) value;
            case Types.DATE:
                return TypedValueUtils.getTimestampFromDate( (Date) value );
            case Types.TIME:
                return TypedValueUtils.getTimestampFromTime( (Time) value );
        }
        if ( TypedValueUtils.isStringRepresented( jdbcType ) ) {
            return TypedValueUtils.getTimestampFromString( (String) value );
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to time" );
    }


    @Override
    public Timestamp asTimestamp( Calendar calendar ) throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        return TypedValueUtils.getTimestampInCalendar( asTimestamp(), calendar );
    }


    @Override
    public Ref asRef() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof Ref ) {
            return (Ref) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a ref" );
    }


    @Override
    public RowId asRowId() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof RowId ) {
            return (RowId) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a row id" );
    }


    @Override
    public URL asUrl() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof URL ) {
            return (URL) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a url" );
    }


    @Override
    public NClob asNClob() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof NClob ) {
            return (NClob) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a nclob" );
    }


    @Override
    public SQLXML asSQLXML() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        if ( value instanceof SQLXML ) {
            return (SQLXML) value;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Can't convert this value to a nclob" );
    }


    @Override
    public String asNString() throws SQLException {
        return asString();
    }


    @Override
    public Reader asNCharacterStream() throws SQLException {
        return asCharacterStream();
    }


    public boolean isUdtPrototype() {
        if ( internalType == null ) {
            return false;
        }
        return internalType.equals( UDT_PROOTYPE_TYPE );
    }


    public UDTPrototype getUdtPrototype() throws ProtoInterfaceServiceException {
        if ( !isUdtPrototype() ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "This typed value does not represent a udt prototype" );
        }
        if ( !(value instanceof UDTPrototype) ) {
            throw new ProtoInterfaceServiceException( "Should never be thrown" );
        }
        return (UDTPrototype) value;
    }


    @Override
    public Object asObject() throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        switch ( jdbcType ) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return asString();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return asBigDecimal();
            case Types.BIT:
            case Types.BOOLEAN:
                return asBoolean();
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return asInt();
            case Types.BIGINT:
                return asLong();
            case Types.REAL:
                return asFloat();
            case Types.FLOAT:
            case Types.DOUBLE:
                return asDouble();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return asBytes();
            case Types.DATE:
                return asDate();
            case Types.TIME:
                return asTime();
            case Types.TIMESTAMP:
                return asTimestamp();
            case Types.DISTINCT:
                // should return object type of underlying type
                throw new IllegalArgumentException( "Retrieval of Types.DISTINCT not implemented" );
            case Types.CLOB:
                return asClob();
            case Types.BLOB:
                return asBlob();
            case Types.ARRAY:
                return asArray();
            case Types.STRUCT:
                return asStruct();
            case Types.REF:
                return asRef();
            case Types.DATALINK:
                return asUrl();
            case Types.JAVA_OBJECT:
                return getValue();
            case Types.ROWID:
                return asRowId();
            case Types.NCLOB:
                return asNClob();
            case Types.SQLXML:
                return asSQLXML();
        }
        throw new IllegalArgumentException( "No conversion to object possible for jdbc type: " + getJdbcType() );
    }

    @Override
    public Object asObject( Calendar calendar ) throws SQLException {
        if ( isSqlNull() || isNull() ) {
            return null;
        }
        switch ( jdbcType ) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return asString();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return asBigDecimal();
            case Types.BIT:
            case Types.BOOLEAN:
                return asBoolean();
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return asInt();
            case Types.BIGINT:
                return asLong();
            case Types.REAL:
                return asFloat();
            case Types.FLOAT:
            case Types.DOUBLE:
                return asDouble();
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return asBytes();
            case Types.DATE:
                return asDate(calendar);
            case Types.TIME:
                return asTime(calendar);
            case Types.TIMESTAMP:
                return asTimestamp(calendar);
            case Types.DISTINCT:
                // should return object type of underlying type
                throw new IllegalArgumentException( "Retrieval of Types.DISTINCT not implemented" );
            case Types.CLOB:
                return asClob();
            case Types.BLOB:
                return asBlob();
            case Types.ARRAY:
                return asArray();
            case Types.STRUCT:
                return asStruct();
            case Types.REF:
                return asRef();
            case Types.DATALINK:
                return asUrl();
            case Types.JAVA_OBJECT:
                return getValue();
            case Types.ROWID:
                return asRowId();
            case Types.NCLOB:
                return asNClob();
            case Types.SQLXML:
                return asSQLXML();
        }
        throw new IllegalArgumentException( "No conversion to object possible for jdbc type: " + getJdbcType() );
    }


    public <T> T asObject( Class<T> aClass ) throws ProtoInterfaceServiceException {
        if ( !isUdtPrototype() ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "This typed value does not represent a udt prototype" );
        }
        return aClass.cast( buildFromUdtPrototype( aClass, getUdtPrototype() ) );
    }


    @Override
    public Object asObject( Map<String, Class<?>> map ) throws SQLException {
        return buildFromUdtPrototype( map );
    }


    private Object buildFromUdtPrototype( Map<String, Class<?>> map ) throws SQLException {
        if ( value == null ) {
            return null;
        }
        UDTPrototype prototype = getUdtPrototype();
        Class<?> udtClass = map.get( prototype.getTypeName() );
        return buildFromUdtPrototype( udtClass, prototype );
    }


    private <T> Object buildFromUdtPrototype( Class<T> udtClass, UDTPrototype prototype ) throws ProtoInterfaceServiceException {
        if ( udtClass == null ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Type-map contains no type for internal type " + prototype.getTypeName() );
        }
        try {
            Constructor<?> udtConstructor = udtClass.getConstructor( SQLInput.class, String.class );
            Object value = udtConstructor.newInstance( prototype, prototype.getTypeName() );
            internalType = prototype.getTypeName();
            return value;
        } catch ( NoSuchMethodException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.MISSING_INTERFACE, "The type contained in the type map does not implement the SQLInput interface required for udt construction" );
        } catch ( InvocationTargetException | InstantiationException | IllegalAccessException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.UDT_CONSTRUCTION_FAILED, "Construction of user defined type failed", e );
        }
    }


    public static TypedValue fromObject( Object value ) throws SQLException {
        try {
            return TypedValueUtils.buildTypedValueFromObject( value );
        } catch ( ParseException | SQLFeatureNotSupportedException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion from object failed.", e );
        }
    }


    public static TypedValue fromObject( Object value, int targetSqlType ) throws SQLException {
        try {
            return TypedValueUtils.buildTypedValueFromObject( value, targetSqlType );
        } catch ( ParseException | SQLFeatureNotSupportedException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Conversion from object failed.", e );
        }
    }


    public static TypedValue fromObject( Object value, int targetSqlType, int scaleOrLength ) throws NotImplementedException {
        return TypedValueUtils.fromObject( value, targetSqlType, scaleOrLength );
    }


    public TypedValue getTrimmed( int length ) {
        switch ( jdbcType ) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                byte[] binaryData = Arrays.copyOfRange( (byte[]) value, 0, length );
                return TypedValue.fromBytes( binaryData );
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
                String stringData = ((String) value).substring( 0, length );
                return TypedValue.fromString( stringData );
        }
        return this;
    }

}


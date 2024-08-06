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

import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URL;
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
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.properties.DriverProperties;
import org.polypheny.jdbc.streaming.BinaryPrismOutputStream;
import org.polypheny.jdbc.streaming.BlobPrismOutputStream;
import org.polypheny.jdbc.streaming.BinaryPrismInputStream;
import org.polypheny.jdbc.streaming.StreamingIndex;
import org.polypheny.jdbc.streaming.StringPrismInputStream;
import org.polypheny.jdbc.utils.ProtoUtils;
import org.polypheny.jdbc.utils.TypedValueUtils;
import org.polypheny.prism.ProtoBigDecimal;
import org.polypheny.prism.ProtoBinary;
import org.polypheny.prism.ProtoBoolean;
import org.polypheny.prism.ProtoDate;
import org.polypheny.prism.ProtoDouble;
import org.polypheny.prism.ProtoFile;
import org.polypheny.prism.ProtoFloat;
import org.polypheny.prism.ProtoInteger;
import org.polypheny.prism.ProtoInterval;
import org.polypheny.prism.ProtoList;
import org.polypheny.prism.ProtoLong;
import org.polypheny.prism.ProtoNull;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoTime;
import org.polypheny.prism.ProtoTimestamp;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.ProtoValue.ValueCase;

public class TypedValue implements Convertible {

    private static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;

    public static final int STREAMING_THRESHOLD = 100000000;


    private static final Set<ValueCase> customTypes = new HashSet<>( Arrays.asList(
            ValueCase.DOCUMENT,
            ValueCase.INTERVAL
    ) );

    @Setter
    private PolyConnection connection;

    private ProtoValue serialized;
    @Getter
    private ProtoValue.ValueCase valueCase;
    private boolean isSerialized = true;

    private Boolean booleanValue;
    private Integer integerValue;
    private Long bigintValue;
    private Float floatValue;
    private Double doubleValue;
    private BigDecimal bigDecimalValue;
    private byte[] binaryValue;
    private Blob blobValue;
    private Date dateValue;
    private Time timeValue;
    private Timestamp timestampValue;
    private String varcharValue;
    private Array arrayValue;
    private RowId rowIdValue;
    private Object otherValue;


    public TypedValue( ProtoValue value, PolyConnection polyConnection ) {
        this.connection = polyConnection;
        this.serialized = value;
        this.valueCase = serialized.getValueCase();
    }


    private TypedValue() {
        this.isSerialized = false;
    }


    public static TypedValue fromBoolean( boolean booleanValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.BOOLEAN;
        value.booleanValue = booleanValue;
        return value;
    }


    public static TypedValue fromByte( byte byteValue ) {
        return fromShort( byteValue );
    }


    public static TypedValue fromShort( short shortValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.INTEGER;
        value.integerValue = (int) shortValue;
        return value;
    }


    public static TypedValue fromInteger( int intValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.INTEGER;
        value.integerValue = intValue;
        return value;
    }


    public static TypedValue fromLong( long bigintValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.LONG;
        value.bigintValue = bigintValue;
        return value;
    }


    public static TypedValue fromFloat( float floatValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.FLOAT;
        value.floatValue = floatValue;
        return value;
    }


    public static TypedValue fromDouble( double doubleValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.DOUBLE;
        value.doubleValue = doubleValue;
        return value;
    }


    public static TypedValue fromBigDecimal( BigDecimal bigDecimalValue ) {
        if ( bigDecimalValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.BIG_DECIMAL;
        value.bigDecimalValue = bigDecimalValue;
        return value;
    }


    public static TypedValue fromString( String stringValue ) {
        if ( stringValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.STRING;
        value.varcharValue = stringValue;
        return value;
    }


    public static TypedValue fromBytes( byte[] binaryValue ) {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.BINARY;
        value.binaryValue = binaryValue;
        return value;
    }


    public static TypedValue fromDate( Date dateValue ) {
        if ( dateValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.DATE;
        value.dateValue = dateValue;
        return value;
    }


    public static TypedValue fromDate( Date dateValue, Calendar calendar ) {
        return fromDate( TypedValueUtils.getDateInCalendar( dateValue, calendar ) );
    }


    public static TypedValue fromTime( Time timeValue ) {
        if ( timeValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.TIME;
        value.timeValue = timeValue;
        return value;
    }


    public static TypedValue fromTime( Time timeValue, Calendar calendar ) {
        return fromTime( TypedValueUtils.getTimeInCalendar( timeValue, calendar ) );
    }


    public static TypedValue fromTimestamp( Timestamp timestampValue ) {
        if ( timestampValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.TIMESTAMP;
        value.timestampValue = timestampValue;
        return value;
    }


    public static TypedValue fromTimestamp( Timestamp timestampValue, Calendar calendar ) {
        return fromTimestamp( TypedValueUtils.getTimestampInCalendar( timestampValue, calendar ) );
    }


    public static TypedValue fromAsciiStream( InputStream asciiStream, int length ) throws SQLException {
        return fromAsciiStream( asciiStream );
    }


    public static TypedValue fromAsciiStream( InputStream asciiStream, long length ) throws SQLException {
        return fromAsciiStream( asciiStream );
    }


    public static TypedValue fromAsciiStream( InputStream asciiStream ) throws SQLException {
        try {
            return fromString( new String( collectByteStream( asciiStream ), StandardCharsets.US_ASCII ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read from ascii stream.", e );
        }
    }


    public static TypedValue fromUnicodeStream( InputStream unicodeStream, int length ) throws SQLException {
        try {
            return fromString( new String( collectByteStream( unicodeStream ), StandardCharsets.UTF_8 ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read from unicode stream.", e );
        }
    }


    public static TypedValue fromBinaryStream( InputStream binaryStream, int length ) throws SQLException {
        return fromBinaryStream( binaryStream );
    }


    public static TypedValue fromBinaryStream( InputStream binaryStream, long length ) throws SQLException {
        return fromBinaryStream( binaryStream );
    }


    public static TypedValue fromBinaryStream( InputStream binaryStream ) throws SQLException {
        try {
            return fromBytes( collectByteStream( binaryStream ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read from binary stream.", e );
        }
    }


    public static TypedValue fromCharacterStream( Reader characterStream, int length ) throws SQLException {
        return fromCharacterStream( characterStream );
    }


    public static TypedValue fromCharacterStream( Reader characterStream, long length ) throws SQLException {
        return fromCharacterStream( characterStream );
    }


    public static TypedValue fromCharacterStream( Reader characterStream ) throws SQLException {
        try {
            return fromString( collectCharacterStream( characterStream ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read from character stream.", e );
        }
    }


    public static TypedValue fromRef( Ref refValue ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "Refs are not supported yet." );
    }


    public static TypedValue fromDocument( PolyDocument document ) {
        if ( document == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.DOCUMENT;
        value.otherValue = document;
        return value;
    }


    public static TypedValue fromInterval( PolyInterval interval ) {
        if ( interval == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.INTERVAL;
        value.otherValue = interval;
        return value;
    }


    public static TypedValue fromBlob( Blob blobValue ) {
        if ( blobValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.FILE;
        value.blobValue = blobValue;
        return value;
    }


    public static TypedValue fromBlob( InputStream binaryStream ) throws SQLException {
        try {
            return fromBlob( new PolyBlob( collectByteStream( binaryStream ) ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read blob form binary stream.", e );
        }
    }


    public static TypedValue fromBlob( InputStream binaryStream, long length ) throws SQLException {
        return fromBlob( binaryStream );
    }


    public static TypedValue fromNull() {
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.NULL;
        return value;
    }


    public static TypedValue fromClob( Clob clobValue ) throws SQLException {
        try {
            return fromString( collectCharacterStream( clobValue.getCharacterStream() ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read data from clob.", e );
        }
    }


    public static TypedValue fromClob( Reader reader ) throws SQLException {
        try {
            return fromString( collectCharacterStream( reader ) );
        } catch ( IOException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.STREAM_ERROR, "Failed to read data from streamed clob.", e );
        }
    }


    public static TypedValue fromClob( Reader reader, long length ) throws SQLException {
        return fromClob( reader );
    }


    public static TypedValue fromArray( Array arrayValue ) {
        if ( arrayValue == null ) {
            return fromNull();
        }
        TypedValue value = new TypedValue();
        value.valueCase = ValueCase.LIST;
        value.arrayValue = arrayValue;
        return value;
    }


    public static TypedValue fromUrl( URL urlValue ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "URLs are not supported yet." );
    }


    public static TypedValue fromRowId( RowId rowIdValue ) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException( "RowIds are not supported yet." );
    }


    public static TypedValue fromObject( Object value ) throws SQLException {
        try {
            return TypedValueUtils.buildTypedValueFromObject( value );
        } catch ( ParseException | SQLFeatureNotSupportedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Conversion from object failed.", e );
        }
    }


    public static TypedValue fromObject( Object value, int targetSqlType ) throws SQLException {
        try {
            return TypedValueUtils.buildTypedValueFromObject( value, targetSqlType );
        } catch ( ParseException | SQLFeatureNotSupportedException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Conversion from object failed.", e );
        }
    }


    public static TypedValue fromObject( Object value, int targetSqlType, int scaleOrLength ) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported yet." );
    }


    public static TypedValue fromNString( String stringValue ) {
        return fromString( stringValue );
    }


    public static TypedValue fromNCharacterStream( Reader character ) throws SQLException {
        return fromCharacterStream( character );
    }


    public static TypedValue fromNCharacterStream( Reader characterStream, long length ) throws SQLException {
        return fromCharacterStream( characterStream, length );
    }


    public static TypedValue fromNClob( NClob nClobValue ) throws SQLException {
        return fromClob( nClobValue.getCharacterStream() );
    }


    public static TypedValue fromNClob( Reader characterStream ) throws SQLException {
        return fromClob( characterStream );
    }


    public static TypedValue fromNClob( Reader characterStream, int length ) throws SQLException {
        return fromClob( characterStream, length );
    }


    public static TypedValue fromNClob( Reader characterStream, long length ) throws SQLException {
        return fromClob( characterStream, length );
    }


    public static TypedValue fromSQLXML( SQLXML sqlxmlValue ) throws SQLException {
        throw new SQLFeatureNotSupportedException( "SQLXML is not yet supported." );
    }


    public static TypedValue fromStruct( Struct value ) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException( "Structs are not yet supported." );
    }


    @Override
    public boolean isNull() {
        return valueCase == ValueCase.NULL;
    }


    public boolean isUdt() {
        //TODO: adjust when user defined types are supported
        return false;
    }


    public int getLength() {
        if ( isSerialized ) {
            deserialize();
        }
        switch ( valueCase ) {
            case BINARY:
                return binaryValue.length;
            case STRING:
                return varcharValue.length();
        }
        return 0;
    }


    public TypedValue getTrimmed( int length ) {
        switch ( valueCase ) {
            case BINARY:
                byte[] binaryData = Arrays.copyOfRange( binaryValue, 0, length );
                return TypedValue.fromBytes( binaryData );
            case STRING:
                String string = varcharValue.substring( 0, length );
                return TypedValue.fromString( string );
        }
        return this;
    }


    @Override
    public String asString() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return varcharValue;
        }
        if ( isNull() ) {
            return null;
        }
        switch ( valueCase ) {
            case BOOLEAN:
                return booleanValue ? "1" : "0";
            case INTEGER:
                return integerValue.toString();
            case LONG:
                return bigintValue.toString();
            case BIG_DECIMAL:
                return bigDecimalValue.toString();
            case FLOAT:
                return floatValue.toString();
            case DOUBLE:
                return doubleValue.toString();
            case DATE:
                return dateValue.toString();
            case TIME:
                return timeValue.toString();
            case TIMESTAMP:
                return timestampValue.toString();
            case INTERVAL:
                return ((PolyInterval) otherValue).toString();
            case BINARY:
                return Arrays.toString( binaryValue );
            case NULL:
                return null;
            case LIST:
            case FILE:
            case DOCUMENT:
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value cannot be returned as a string." );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value cannot be returned as a string." );
    }


    @Override
    public boolean asBoolean() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( booleanValue != null ) {
            return booleanValue;
        }
        if ( varcharValue != null ) {
            if ( varcharValue.equals( "0" ) ) {
                return false;
            }
            if ( varcharValue.equals( "1" ) ) {
                return true;
            }
        }
        if ( integerValue != null ) {
            if ( integerValue == 0 ) {
                return false;
            }
            if ( integerValue == 1 ) {
                return true;
            }
        }
        if ( bigintValue != null ) {
            if ( bigintValue == 0 ) {
                return false;
            }
            if ( bigintValue == 1 ) {
                return true;
            }
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type BOOLEAN." );
    }


    @Override
    public byte asByte() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( integerValue != null ) {
            return integerValue.byteValue();
        }
        if ( bigintValue != null ) {
            return bigintValue.byteValue();
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TINYINT, SMALLINT, INTEGER or BIGINT." );
    }


    @Override
    public short asShort() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( integerValue != null ) {
            return integerValue.shortValue();
        }
        if ( bigintValue != null ) {
            return bigintValue.shortValue();
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TINYINT, SMALLINT, INTEGER or BIGINT." );
    }


    @Override
    public int asInt() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( integerValue != null ) {
            return integerValue;
        }
        if ( bigintValue != null ) {
            return bigintValue.intValue();
        }
        if ( bigDecimalValue != null ) {
            return bigDecimalValue.intValue();
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TINYINT, SMALLINT, INTEGER or BIGINT." );
    }


    @Override
    public long asLong() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( bigintValue != null ) {
            return bigintValue;
        }
        if ( integerValue != null ) {
            return integerValue;
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TINYINT, SMALLINT, INTEGER or BIGINT." );
    }


    @Override
    public float asFloat() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( floatValue != null ) {
            return floatValue;
        }
        if ( doubleValue != null ) {
            return doubleValue.floatValue();
        }
        if ( bigDecimalValue != null ) {
            return bigDecimalValue.floatValue();
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type REAL, FLOT or DOUBLE." );
    }


    @Override
    public double asDouble() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( doubleValue != null ) {
            return doubleValue.doubleValue();
        }
        if ( floatValue != null ) {
            return floatValue.doubleValue();
        }
        if ( bigDecimalValue != null ) {
            return bigDecimalValue.doubleValue();
        }
        if ( isNull() ) {
            return 0;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type REAL, FLOT or DOUBLE." );
    }


    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( bigDecimalValue != null ) {
            return bigDecimalValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type DECIMAL." );
    }


    @Override
    @Deprecated
    public BigDecimal asBigDecimal( int scale ) throws SQLException {
        return asBigDecimal().setScale( scale, RoundingMode.HALF_EVEN );
    }


    @Override
    public byte[] asBytes() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( binaryValue != null ) {
            return binaryValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type BINARY or VARBINARY." );
    }


    @Override
    public InputStream asAsciiStream() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return new ByteArrayInputStream( varcharValue.getBytes( StandardCharsets.US_ASCII ) );
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type CHAR or VARCHAR." );
    }


    @Override
    @Deprecated
    public InputStream asUnicodeStream() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return new ByteArrayInputStream( varcharValue.getBytes( StandardCharsets.UTF_8 ) );
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type CHAR or VARCHAR." );
    }


    @Override
    public InputStream asBinaryStream() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( blobValue != null ) {
            return blobValue.getBinaryStream();
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not streamable." );
    }


    @Override
    public PolyDocument asDocument() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( otherValue != null ) {
            return (PolyDocument) otherValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type DOCUMENT." );
    }


    @Override
    public PolyInterval asInterval() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( otherValue != null ) {
            return (PolyInterval) otherValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type INTERVAL." );
    }


    @Override
    public Object asObject() throws SQLException {
        switch ( valueCase ) {
            case BOOLEAN:
                return asBoolean();
            case INTEGER:
                return asInt();
            case LONG:
                return asLong();
            case BIG_DECIMAL:
                return asBigDecimal();
            case FLOAT:
                return asFloat();
            case DOUBLE:
                return asDouble();
            case DATE:
                return asDate();
            case TIME:
                return asTime();
            case TIMESTAMP:
                return asTimestamp();
            case INTERVAL:
                return asInterval();
            case STRING:
                return asString();
            case BINARY:
                return asBytes();
            case NULL:
                return null;
            case LIST:
                return asArray();
            case DOCUMENT:
                return asDocument();
            case FILE:
                return asBlob();
            default:
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value has unknown type and thus can not be returned." );
        }
    }


    @Override
    public Object asObject( Calendar calendar ) throws SQLException {
        switch ( valueCase ) {
            case BOOLEAN:
                return asBoolean();
            case INTEGER:
                return asInt();
            case LONG:
                return asLong();
            case BIG_DECIMAL:
                return asBigDecimal();
            case FLOAT:
                return asFloat();
            case DOUBLE:
                return asDouble();
            case DATE:
                return asDate( calendar );
            case TIME:
                return asTime( calendar );
            case TIMESTAMP:
                return asTimestamp( calendar );
            case INTERVAL:
                return asInterval();
            case STRING:
                return asString();
            case BINARY:
                return asBytes();
            case NULL:
                return null;
            case LIST:
                return asArray();
            case DOCUMENT:
                return asDocument();
            case FILE:
                return asBlob();
            default:
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value has unknown type and thus can not be returned." );
        }
    }


    @Override
    public Reader asCharacterStream() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return new StringReader( varcharValue );
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type CHAR or VARCHAR." );
    }


    @Override
    public Blob asBlob() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( blobValue != null ) {
            return blobValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type FILE, AUDIO, VIDEO or IMAGE." );
    }


    @Override
    public Clob asClob() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return new PolyClob( varcharValue );
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type CHAR or VARCHAR." );
    }


    @Override
    public Array asArray() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( arrayValue != null ) {
            return arrayValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type ARRAY." );
    }


    @Override
    public Struct asStruct() throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "No type retrievable as a struct exists in Polypheny." );
    }


    @Override
    public Date asDate() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( dateValue != null ) {
            return dateValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type DATE." );
    }


    @Override
    public Date asDate( Calendar calendar ) throws SQLException {
        return TypedValueUtils.getDateInCalendar( asDate(), calendar );
    }


    @Override
    public Time asTime() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( timeValue != null ) {
            return timeValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TIME." );
    }


    @Override
    public Time asTime( Calendar calendar ) throws SQLException {
        return TypedValueUtils.getTimeInCalendar( asTime(), calendar );
    }


    @Override
    public Timestamp asTimestamp() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( timestampValue != null ) {
            return timestampValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type TIMESTAMP." );
    }


    @Override
    public Timestamp asTimestamp( Calendar calendar ) throws SQLException {
        return TypedValueUtils.getTimestampInCalendar( asTimestamp(), calendar );
    }


    @Override
    public Ref asRef() throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "No type retrievable as a reference exists in Polypheny." );
    }


    @Override
    public RowId asRowId() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( rowIdValue != null ) {
            return rowIdValue;
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type ROW_ID." );
    }


    @Override
    public URL asUrl() throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "No type retrievable as a url exists in Polypheny." );
    }


    @Override
    public NClob asNClob() throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( varcharValue != null ) {
            return new PolyClob( varcharValue );
        }
        if ( isNull() ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type FILE, AUDIO, VIDEO or IMAGE." );
    }


    @Override
    public SQLXML asSQLXML() throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "No type retrievable as SQLXML exists in Polypheny." );
    }


    @Override
    public String asNString() throws SQLException {
        return asString();
    }


    @Override
    public Reader asNCharacterStream() throws SQLException {
        return asCharacterStream();
    }


    @Override
    public Object asObject( Map<String, Class<?>> map ) throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( otherValue == null || !(otherValue instanceof UDTPrototype) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type USER_DEFINED_TYPE." );
        }
        UDTPrototype prototype = (UDTPrototype) otherValue;
        Class<?> udtClass = map.get( prototype.getTypeName() );
        return buildFromUdtPrototype( udtClass, prototype );
    }


    @Override
    public <T> T asObject( Class<T> aClass ) throws SQLException {
        if ( isSerialized ) {
            deserialize();
        }
        if ( otherValue == null || !(otherValue instanceof UDTPrototype) ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "This value is not of type USER_DEFINED_TYPE." );
        }
        return aClass.cast( buildFromUdtPrototype( aClass, (UDTPrototype) otherValue ) );
    }


    private <T> Object buildFromUdtPrototype( Class<T> udtClass, UDTPrototype prototype ) throws SQLException {
        if ( udtClass == null ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Type-map contains no type for internal type " + prototype.getTypeName() );
        }
        try {
            Constructor<?> udtConstructor = udtClass.getConstructor( SQLInput.class, String.class );
            return udtConstructor.newInstance( prototype, prototype.getTypeName() );
        } catch ( NoSuchMethodException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.MISSING_INTERFACE, "The type contained in the type map does not implement the SQLInput interface required for udt construction" );
        } catch ( InvocationTargetException | InstantiationException | IllegalAccessException e ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.UDT_CONSTRUCTION_FAILED, "Construction of user defined type failed", e );
        }
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


    private void deserialize() {
        try {
            switch ( valueCase ) {
                case BOOLEAN:
                    booleanValue = serialized.getBoolean().getBoolean();
                    break;
                case INTEGER:
                    integerValue = serialized.getInteger().getInteger();
                    break;
                case LONG:
                    bigintValue = serialized.getLong().getLong();
                    break;
                case BINARY:
                    binaryValue = deserializeToBinary( serialized.getBinary() );
                    break;
                case DATE:
                    dateValue = new Date( serialized.getDate().getDate() * MILLISECONDS_PER_DAY );
                    break;
                case DOUBLE:
                    doubleValue = serialized.getDouble().getDouble();
                    break;
                case FLOAT:
                    floatValue = serialized.getFloat().getFloat();
                    break;
                case NULL:
                    break;
                case STRING:
                    varcharValue = deserializeToString( serialized.getString() );
                    break;
                case TIME:
                    timeValue = new Time( serialized.getTime().getTime() );
                    break;
                case TIMESTAMP:
                    timestampValue = new Timestamp( serialized.getTimestamp().getTimestamp() );
                    break;
                case BIG_DECIMAL:
                    bigDecimalValue = getBigDecimal( serialized.getBigDecimal().getUnscaledValue(), serialized.getBigDecimal().getScale() );
                    break;
                case LIST:
                    arrayValue = getArray( serialized, connection );
                    break;
                case INTERVAL:
                    otherValue = getInterval( serialized.getInterval() );
                    break;
                case DOCUMENT:
                    otherValue = new PolyDocument( serialized.getDocument(), connection );
                    break;
                case FILE:
                    blobValue = new PolyBlob( serialized.getFile(), connection );
                    break;
                default:
                    throw new RuntimeException( "Cannot deserialize ProtoValue of case " + valueCase );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    private byte[] deserializeToBinary( ProtoBinary protoBinary ) {
        if ( protoBinary.hasBinary() ) {
            return protoBinary.getBinary().toByteArray();
        }
        InputStream stream = new BinaryPrismInputStream( protoBinary.getStatementId(), protoBinary.getStreamId(), protoBinary.getIsForwardOnly(), connection );
        try {
            return collectByteStream( stream );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    private String deserializeToString( ProtoString protoString ) {
        if ( protoString.hasString() ) {
            return protoString.getString();
        }
        Reader stream = new StringPrismInputStream(protoString.getStatementId(), protoString.getStreamId(), protoString.getIsForwardOnly(), connection);
        try {
            return collectCharacterStream( stream );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    public ProtoValue serialize( StreamingIndex streamingIndex ) throws SQLException {
        switch ( valueCase ) {
            case BOOLEAN:
                return serializeAsProtoBoolean();
            case INTEGER:
                return serializeAsProtoInteger();
            case LONG:
                return serializeAsProtoLong();
            case BIG_DECIMAL:
                return serializeAsProtoBigDecimal();
            case FLOAT:
                return serializeAsProtoFloat();
            case DOUBLE:
                return serializeAsProtoDouble();
            case DATE:
                return serializeAsProtoDate();
            case TIME:
                return serializeAsProtoTime();
            case TIMESTAMP:
                return serializeAsTimestamp();
            case INTERVAL:
                return serializeAsInterval();
            case STRING:
                return serializeAsProtoString();
            case BINARY:
                return serializeAsProtoBinary( streamingIndex );
            case NULL:
                return serializeAsProtoNull();
            case LIST:
                return serializeAsProtoList( streamingIndex );
            case FILE:
                return serializeAsProtoFile( streamingIndex );
            case DOCUMENT:
                return serializeAsProtoDocument( streamingIndex );
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Failed to serialize unknown type: " + valueCase.name() );
    }


    private ProtoValue serializeAsProtoFile( StreamingIndex streamingIndex ) throws SQLException {
        ProtoFile protoFile;
        if ( blobValue.length() < STREAMING_THRESHOLD ) {
            try {
                protoFile = ProtoFile.newBuilder()
                        .setBinary( ByteString.copyFrom( collectByteStream( blobValue.getBinaryStream() ) ) )
                        .build();
                return ProtoValue.newBuilder()
                        .setFile( protoFile )
                        .build();
            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }
        }
        long streamId = streamingIndex.register( new BlobPrismOutputStream( blobValue ) );
        protoFile = ProtoFile.newBuilder()
                .setStreamId( streamId )
                .setIsForwardOnly( true )
                .build();
        return ProtoValue.newBuilder()
                .setFile( protoFile )
                .build();
    }


    private ProtoValue serializeAsProtoDocument( StreamingIndex streamingIndex ) {
        return ProtoValue.newBuilder()
                .setDocument( ((PolyDocument) otherValue).serialize( streamingIndex ) )
                .build();
    }


    private ProtoValue serializeAsInterval() {
        PolyInterval interval = (PolyInterval) otherValue;
        ProtoInterval protoInterval = ProtoInterval.newBuilder()
                .setMonths( interval.getMonths() )
                .setMilliseconds( interval.getMilliseconds() )
                .build();
        return ProtoValue.newBuilder()
                .setInterval( protoInterval )
                .build();
    }


    private ProtoValue serializeAsProtoList( StreamingIndex streamingIndex ) throws SQLException {
        List<ProtoValue> elements = new ArrayList<>();
        for ( Object object : (Object[]) arrayValue.getArray() ) {
            elements.add( TypedValue.fromObject( object ).serialize( streamingIndex ) );
        }
        ProtoList protoList = ProtoList.newBuilder()
                .addAllValues( elements )
                .build();
        return ProtoValue.newBuilder()
                .setList( protoList )
                .build();
    }


    private ProtoValue serializeAsProtoDouble() {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( doubleValue )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .build();
    }


    private ProtoValue serializeAsProtoFloat() {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( floatValue )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .build();
    }


    private ProtoValue serializeAsProtoLong() {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( bigintValue )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .build();
    }


    private ProtoValue serializeAsProtoBigDecimal() {
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( bigDecimalValue.unscaledValue().toByteArray() ) )
                .setScale( bigDecimalValue.scale() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .build();
    }


    private ProtoValue serializeAsProtoDate() {
        long milliseconds = dateValue.getTime();
        milliseconds += DriverProperties.getDEFAULT_TIMEZONE().getOffset( milliseconds );
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( milliseconds / MILLISECONDS_PER_DAY )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .build();
    }


    private ProtoValue serializeAsProtoString() {
        return ProtoUtils.serializeAsProtoString( varcharValue );
    }


    private ProtoValue serializeAsProtoTime() {
        long ofDay = timeValue.getTime();
        ofDay += DriverProperties.getDEFAULT_TIMEZONE().getOffset( ofDay );
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setTime( (int) ofDay )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .build();
    }


    private ProtoValue serializeAsTimestamp() {
        long milliseconds = timestampValue.getTime();
        milliseconds += DriverProperties.getDEFAULT_TIMEZONE().getOffset( milliseconds );
        ProtoTimestamp protoTimestamp = ProtoTimestamp.newBuilder()
                .setTimestamp( milliseconds )
                .build();
        return ProtoValue.newBuilder()
                .setTimestamp( protoTimestamp )
                .build();
    }


    private ProtoValue serializeAsProtoBinary( StreamingIndex streamingIndex ) {
        ProtoBinary protoBinary;
        if ( binaryValue.length < STREAMING_THRESHOLD ) {
            protoBinary = ProtoBinary.newBuilder()
                    .setBinary( ByteString.copyFrom( binaryValue ) )
                    .build();
            return ProtoValue.newBuilder()
                    .setBinary( protoBinary )
                    .build();
        }
        long streamId = streamingIndex.register( new BinaryPrismOutputStream( binaryValue ) );
        protoBinary = ProtoBinary.newBuilder()
                .setStreamId( streamId )
                .setIsForwardOnly( true )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    private ProtoValue serializeAsProtoNull() {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .build();
    }


    private ProtoValue serializeAsProtoBoolean() {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( booleanValue )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }


    private ProtoValue serializeAsProtoInteger() {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( integerValue )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .build();
    }


    private static BigDecimal getBigDecimal( ByteString unscaledValue, int scale ) {
        BigInteger value = new BigInteger( unscaledValue.toByteArray() );
        return new BigDecimal( value, scale );
    }


    private static Array getArray( ProtoValue value, PolyConnection polyConnection ) throws SQLException {
        String baseType = value.getValueCase().name();
        List<TypedValue> values = value.getList().getValuesList().stream()
                .map( v -> new TypedValue( v, polyConnection ) )
                .collect( Collectors.toList() );
        return new PolyArray( baseType, values );
    }


    private static PolyInterval getInterval( ProtoInterval interval ) {
        return new PolyInterval( interval.getMonths(), interval.getMilliseconds() );
    }

}

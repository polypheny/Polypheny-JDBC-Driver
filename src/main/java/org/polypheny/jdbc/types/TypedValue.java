package org.polypheny.jdbc.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
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
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.HashMap;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import lombok.Getter;

public class TypedValue implements Convertible {

    @Getter
    private final int jdbcType;
    @Getter
    private final Object value;
    private static final HashMap<Integer, TypedValue> NULL_MAP = new HashMap<>();


    public TypedValue( int jdbcType, Object value ) {
        this.jdbcType = jdbcType;
        this.value = value;
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


    public static TypedValue fromDate( Date value, Calendar calendar ) throws NotImplementedException {
        //TODO TH handle date and calendar
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromTime( Time value ) {
        return new TypedValue( Types.TIME, value );
    }


    public static TypedValue fromTime( Time value, Calendar calendar ) throws NotImplementedException {
        //TODO TH handle time and calendar
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromTimestamp( Timestamp value ) {
        return new TypedValue( Types.TIMESTAMP, value );
    }


    public static TypedValue fromTimestamp( Timestamp value, Calendar calendar ) throws NotImplementedException {
        //TODO TH handle timestamp and calendar
        throw new NotImplementedException( "Not yet implemented..." );
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


    public static TypedValue fromNull( int sqlType, String typeName ) throws NotImplementedException {
        throw new NotImplementedException( "Not implemented yet..." );
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


    public static TypedValue setBlob( InputStream stream, long length ) throws NotImplementedException {
        //TODO TH build BLOB from bytes...
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromSQLXML( SQLXML value ) {
        return new TypedValue( Types.SQLXML, value );
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


    @Override
    public boolean isSqlNull() throws SQLException {
        return jdbcType == Types.NULL;
    }


    @Override
    public boolean isNull() {
        return value == null;
    }


    @Override
    public String asString() throws SQLException {
        if ( value instanceof String ) {
            return (String) value;
        }
        throw new SQLException( "Can't convert this value to string" );
    }


    @Override
    public boolean asBoolean() throws SQLException {
        if ( isSqlNull() ) {
            // jdbc4: if the value is SQL NULL, the value returned is false
            return false;
        }
        switch ( jdbcType ) {
            case Types.BOOLEAN:
                if ( value instanceof Boolean ) {
                    return (Boolean) value;
                }
                throw new SQLException( "Can't convert this value to boolean" );
            case Types.TINYINT:
                if ( value instanceof Byte ) {
                    return (Byte) value != 0;
                }
                throw new SQLException( "Can't convert this value to boolean" );
            case Types.SMALLINT:
                if ( value instanceof Short ) {
                    switch ( (Short) value ) {
                        case 0:
                            return false;
                        case 1:
                            return true;
                    }
                }
                throw new SQLException( "Can't convert this value to boolean" );
            case Types.INTEGER:
                if ( value instanceof Integer ) {
                    switch ( (Integer) value ) {
                        case 0:
                            return false;
                        case 1:
                            return true;
                    }
                }
                throw new SQLException( "Can't convert this value to boolean" );
            case Types.BIGINT:
                if ( value instanceof Long ) {
                    if ( (Long) value == 0 ) {
                        return false;
                    } else if ( (Long) value == 1 ) {
                        return true;
                    }
                }
                throw new SQLException( "Can't convert this value to boolean" );
            case Types.CHAR:
            case Types.VARCHAR:
                if ( value instanceof String ) {
                    if ( value.equals( "0" ) ) {
                        return false;
                    }
                    if ( value.equals( "1" ) ) {
                        return true;
                    }
                }
                throw new SQLException( "Can't convert this value to boolean" );
        }
        throw new SQLException( "Conversion to BOOLEAN is not supported." );
    }


    @Override
    public byte asByte() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Byte ) {
            return (Byte) value;
        }
        throw new SQLException( "Can't convert this value to byte" );
    }


    @Override
    public short asShort() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Short ) {
            return (Short) value;
        }
        throw new SQLException( "Can't convert this value to short" );
    }


    @Override
    public int asInt() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Integer ) {
            return (Integer) value;
        }
        throw new SQLException( "Can't convert this value to int" );
    }


    @Override
    public long asLong() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Long ) {
            return (Long) value;
        }
        throw new SQLException( "Can't convert this value to long" );
    }


    @Override
    public float asFloat() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Float ) {
            return (Float) value;
        }
        throw new SQLException( "Can't convert this value to float" );
    }


    @Override
    public double asDouble() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( value instanceof Double ) {
            return (Double) value;
        }
        throw new SQLException( "Can't convert this value to double" );
    }


    @Override
    public BigDecimal asBigDecimal( int scale ) throws SQLException {
        return asBigDecimal().setScale( scale, RoundingMode.HALF_EVEN );
    }


    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof BigDecimal ) {
            return (BigDecimal) value;
        }
        throw new SQLException( "Can't convert this value to byte" );
    }


    @Override
    public byte[] asBytes() throws SQLException {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream( byteArrayOutputStream );
            objectOutputStream.writeObject( value );
            return byteArrayOutputStream.toByteArray();
        } catch ( IOException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public InputStream asAsciiStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof String ) {
            return new ByteArrayInputStream( ((String) value).getBytes( StandardCharsets.US_ASCII ) );
        }
        throw new SQLException( "Conversion to ascii stream not supported." );
    }


    private byte[] encodeString( String string, Charset charset ) throws SQLException {
        // separate method for error handling
        return string.getBytes( charset );
    }


    @Override
    public InputStream asUnicodeStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof String ) {
            return new ByteArrayInputStream( ((String) value).getBytes( StandardCharsets.UTF_8 ) );
        }
        throw new SQLException( "Conversion to unicode stream not supported." );
    }


    @Override
    public InputStream asBinaryStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof byte[] ) {
            return new ByteArrayInputStream( (byte[]) value );
        }
        throw new SQLException( "Conversion to binary stream not supported." );
    }


    @Override
    public Reader asCharacterStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof String ) {
            return new StringReader( (String) value );
        }
        throw new SQLException( "Can't convert this value to a character stream" );

    }


    @Override
    public Blob asBlob() throws SQLException {
        if ( value instanceof Blob ) {
            return (Blob) value;
        }
        throw new SQLException( "Can't convert this value to a blob" );
    }


    @Override
    public Clob asClob() throws SQLException {
        if ( value instanceof Clob ) {
            return (Clob) value;
        }
        throw new SQLException( "Can't convert this value to a clob" );
    }


    @Override
    public Array asArray() throws SQLException {
        if ( value instanceof Array ) {
            return (Array) value;
        }
        throw new SQLException( "Can't convert this value to an array" );
    }


    @Override
    public Struct asStruct() throws SQLException {
        if ( value instanceof Struct ) {
            return (Struct) value;
        }
        throw new SQLException( "Can't convert this value to a struct" );
    }


    @Override
    public Date asDate() throws SQLException {
        if ( value instanceof java.util.Date ) {
            return (Date) value;
        }
        throw new SQLException( "Can't convert this value to a date" );
    }


    @Override
    public Date asDate( Calendar calendar ) throws SQLException {
        return null;
    }


    public Time asTime() throws SQLException {
        if ( value instanceof Time ) {
            return (Time) value;
        }
        throw new SQLException( "Can't convert this value to time" );
    }


    @Override
    public Time asTime( Calendar calendar ) throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof Time ) {
            long tValue = ((Time) value).getTime();
            return new Time( tValue - calendar.getTimeZone().getOffset( tValue ) );
        }
        throw new SQLException( "Can't convert this value to time" );
    }


    public Timestamp asTimestamp() throws SQLException {
        if ( value instanceof Timestamp ) {
            return (Timestamp) value;
        }
        throw new SQLException( "Can't convert this value to time" );
    }


    @Override
    public Timestamp asTimestamp( Calendar calendar ) throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof Timestamp ) {
            long tsValue = ((Timestamp) value).getTime();
            return new Timestamp( tsValue - calendar.getTimeZone().getOffset( tsValue ) );
        }
        throw new SQLException( "Can't convert this value to a timestamp" );
    }


    @Override
    public URL asUrl() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof URL ) {
            return (URL) value;
        }
        throw new SQLException( "Can't convert this value to a url" );
    }


    @Override
    public NClob asNClob() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof NClob ) {
            return (NClob) value;
        }
        throw new SQLException( "Can't convert this value to a nclob");
    }


    @Override
    public SQLXML asSQLXML() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( value instanceof SQLXML ) {
            return (SQLXML) value;
        }
        throw new SQLException( "Can't convert this value to a nclob");
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
    public Object asObject() {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        //TODO soething
    }


    public static TypedValue fromObject( Object value ) throws NotImplementedException {
        //TODO TH: type conversion
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromObject( Object value, int targetSqlType ) {
        //TODO TH: type conversion
        return null;
    }


    public static TypedValue fromObject( Object value, int targetSqlType, int scaleOrLength ) throws NotImplementedException {
        //TODO TH: type conversion
        throw new NotImplementedException( "Not yet implemented..." );
    }


}

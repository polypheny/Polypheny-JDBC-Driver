package org.polypheny.jdbc.types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
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
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import lombok.Getter;
import org.polypheny.jdbc.proto.ProtoValue;

public class TypedValue implements Convertible {

    @Getter
    private final int jdbcType;
    @Getter
    private final Object value;


    public TypedValue( ProtoValue protoValue ) {
        this.jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( protoValue.getType() );
        this.value = ProtoValueDeserializer.deserialize( protoValue );
    }


    private TypedValue( int jdbcType, Object value ) {
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
        return new TypedValue( Types.VARBINARY, bytes );
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


    public static TypedValue fromNull( int sqlType ) throws NotImplementedException {
        throw new NotImplementedException( "Not implemented yet..." );
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


    public static TypedValue fromObject( Object value ) throws NotImplementedException {
        //TODO TH: type conversion
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromObject( Object value, int targetSqlType ) throws NotImplementedException {
        //TODO TH: type conversion
        throw new NotImplementedException( "Not yet implemented..." );
    }


    public static TypedValue fromObject( Object value, int targetSqlType, int scaleOrLength ) throws NotImplementedException {
        //TODO TH: type conversion
        throw new NotImplementedException( "Not yet implemented..." );
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
        /*
         * According to jdbc this should send the string to th dbms without changing the encoding to support utf-8 and utf-16.
         * As polypheny is Java-based, utf8 and utf16 strings are supported anyway. No special handling required.
         */
        return collectCharacterStream( reader );
    }


    @Override
    public boolean isSqlNull() throws SQLException {
        return jdbcType == Types.NULL;
    }


    @Override
    public String asString() throws SQLException {
        return value.toString();
    }


    @Override
    public boolean asBoolean() throws SQLException {
        if ( isSqlNull() ) {
            // jdbc4: if the value is SQL NULL, the value returned is false
            return false;
        }
        switch ( jdbcType ) {
            case Types.BOOLEAN:
                return (Boolean) value;
            case Types.TINYINT:
                return (Byte) value != 0;
            case Types.SMALLINT:
                short sValue = (short) value;
                switch ( sValue ) {
                    case 0:
                        return false;
                    case 1:
                        return true;
                }
                throw new SQLException( "Cast from SMALLINT " + sValue + " to boolean is not supported." );
            case Types.INTEGER:
                int iValue = (int) value;
                switch ( iValue ) {
                    case 0:
                        return false;
                    case 1:
                        return true;
                }
                throw new SQLException( "Cast from INTEGER " + iValue + " to boolean is not supported." );
            case Types.BIGINT:
                long lValue = (long) value;
                if ( lValue == 0 ) {
                    return false;
                }
                if ( lValue == 1 ) {
                    return true;
                }
                throw new SQLException( "Cast from BIGINT " + lValue + " to boolean is not supported." );
            case Types.CHAR:
                char cValue = (char) value;
                if ( cValue == '0' ) {
                    return false;
                }
                if ( cValue == '1' ) {
                    return true;
                }
                throw new SQLException( "Cast from CHAR " + cValue + " to boolean is not supported." );
            case Types.VARCHAR:
                String strValue = (String) value;
                if ( strValue.equals( "0" ) ) {
                    return false;
                }
                if ( strValue.equals( "1" ) ) {
                    return true;
                }
                throw new SQLException( "Cast from VARCHAR " + strValue + " to boolean is not supported." );
        }
        throw new SQLException( "Conversion to BOOLEAN is not supported." );
    }


    @Override
    public byte asByte() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.TINYINT ) {
            throw new SQLException( "Conversion to byte is not supported." );
        }
        return (byte) value;
    }


    @Override
    public short asShort() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.SMALLINT ) {
            throw new SQLException( "Conversion to short is not supported." );
        }
        return (short) value;
    }


    @Override
    public int asInt() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.INTEGER ) {
            throw new SQLException( "Conversion to int is not supported." );
        }
        return (int) value;
    }


    @Override
    public long asLong() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.BIGINT ) {
            throw new SQLException( "Conversion to long is not supported." );
        }
        return (long) value;
    }


    @Override
    public float asFloat() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.REAL ) {
            throw new SQLException( "Conversion to float is not supported." );
        }
        return (float) value;
    }


    @Override
    public double asDouble() throws SQLException {
        if ( isSqlNull() ) {
            return 0;
        }
        if ( jdbcType != Types.DOUBLE ) {
            throw new SQLException( "Conversion to double is not supported." );
        }
        return (double) value;
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
        if ( jdbcType != Types.DECIMAL ) {
            throw new SQLException( "Conversion to BigDecimal is not supported." );
        }
        return ((BigDecimal) value);
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
        return new ByteArrayInputStream( value.toString().getBytes( StandardCharsets.US_ASCII ) );
    }


    @Override
    public InputStream asUnicodeStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        return new ByteArrayInputStream( value.toString().getBytes( StandardCharsets.UTF_8 ) );
    }


    @Override
    public InputStream asBinaryStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( jdbcType == Types.BINARY || jdbcType == Types.VARBINARY ) {
            return new ByteArrayInputStream( (byte[]) value );
        }
        throw new SQLException( "Conversion to binary stream not supported." );
    }


    @Override
    public Object asObject() {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        return value;
    }


    @Override
    public Reader asCharacterStream() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        return new StringReader( value.toString() );
    }


    @Override
    public Blob asBlob() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public Clob asClob() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public Array asArray() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public Struct asStruct() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public Date asDate( Calendar calendar ) throws SQLException {
        return null;
    }


    @Override
    public Time asTime( Calendar calendar ) throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( jdbcType != Types.TIME ) {
            throw new SQLException( "Conversion to time not supported." );
        }
        long tValue = ((Time) value).getTime();
        return new Time( tValue - calendar.getTimeZone().getOffset( tValue ) );
    }


    @Override
    public Timestamp asTimestamp( Calendar calendar ) throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        if ( jdbcType != Types.TIMESTAMP ) {
            throw new SQLException( "Conversion to time not supported." );
        }
        long tsValue = ((Timestamp) value).getTime();
        return new Timestamp( tsValue - calendar.getTimeZone().getOffset( tsValue ) );
    }


    @Override
    public URL asUrl() throws SQLException {
        if ( jdbcType == Types.NULL ) {
            return null;
        }
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public NClob asNClob() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public SQLXML asSQLXML() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public String asNString() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }


    @Override
    public Reader asNCharacterStream() throws SQLException {
        throw new SQLException( "Conversion to time not supported." );
    }

}

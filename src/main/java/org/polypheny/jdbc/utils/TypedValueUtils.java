package org.polypheny.jdbc.utils;

import static java.util.stream.Collectors.toCollection;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.RowId;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.stream.Collectors;
import jdk.jshell.spi.ExecutionControl.NotImplementedException;
import org.polypheny.jdbc.deserialization.ProtoToJdbcTypeMap;
import org.polypheny.jdbc.deserialization.ProtoValueDeserializer;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.types.ProtoToPolyTypeNameMap;
import org.polypheny.jdbc.types.TypedValue;

public class TypedValueUtils {

    private static final SimpleDateFormat SQL_DATE_FORMAT = new SimpleDateFormat( "dd MMM yyyy" );
    private static final SimpleDateFormat SQL_TIME_FORMAT = new SimpleDateFormat( "HH:mm:ss" );

    private static Time timeFromString( String string ) throws ParseException {
        return new Time( SQL_TIME_FORMAT.parse( string ).getTime() );
    }


    private static Date dateFromString( String string ) throws ParseException {
        return new Date( SQL_DATE_FORMAT.parse( string ).getTime() );
    }


    private static boolean getBooleanFromNumber( Number number ) {
        return number.byteValue() == 1;
    }


    private static Number getNumberFromBoolean( Boolean bool ) {
        if ( bool ) {
            return 1;
        }
        return 0;
    }


    private static String getOneZeroStringFromBoolean( Boolean bool ) {
        if ( bool ) {
            return "1";
        }
        return "0";
    }


    private static Time getTimeFromOffsetTime( OffsetTime offsetTime ) {
        return Time.valueOf( offsetTime.toLocalTime() );
    }


    private static Timestamp getTimestampFromOffsetDateTime( OffsetDateTime offsetDateTime ) {
        return Timestamp.valueOf( offsetDateTime.atZoneSameInstant( ZoneOffset.UTC ).toLocalDateTime() );
    }


    public static List<ArrayList<TypedValue>> buildRows( List<Row> rows ) {
        return rows.stream()
                .map( TypedValueUtils::buildRow )
                .collect( Collectors.toList() );
    }


    public static ArrayList<TypedValue> buildRow( Row row ) {
        return row.getValuesList().stream()
                .map( ProtoValueDeserializer::deserialize )
                .collect( toCollection( ArrayList::new ) );
    }


    public static ArrayList<Integer> getTypes( List<TypedValue> typedValues ) {
        return typedValues.stream().map( TypedValue::getJdbcType ).collect( toCollection( ArrayList::new ) );
    }


    public static int getJdbcTypeFromPolyTypeName( String polyTypeName ) {
        return ProtoToJdbcTypeMap.getJdbcTypeFromProto( ProtoToPolyTypeNameMap.getProtoTypeFromPolyTypeName( polyTypeName ) );
    }


    public static TypedValue buildTypedValueFromObject( Object value ) throws SQLFeatureNotSupportedException, ParseException {
        if ( value instanceof String ) {
            return buildTypedValueFromObject( value, Types.VARCHAR );
        }
        if ( value instanceof BigDecimal ) {
            return buildTypedValueFromObject( value, Types.NUMERIC );
        }
        if ( value instanceof Boolean ) {
            return buildTypedValueFromObject( value, Types.BOOLEAN );
        }
        if ( value instanceof Byte ) {
            return buildTypedValueFromObject( value, Types.TINYINT );
        }
        if ( value instanceof Short ) {
            return buildTypedValueFromObject( value, Types.SMALLINT );
        }
        if ( value instanceof Integer ) {
            return buildTypedValueFromObject( value, Types.INTEGER );
        }
        if ( value instanceof Long ) {
            return buildTypedValueFromObject( value, Types.BIGINT );
        }
        if ( value instanceof Float ) {
            return buildTypedValueFromObject( value, Types.REAL );
        }
        if ( value instanceof Double ) {
            return buildTypedValueFromObject( value, Types.DOUBLE );
        }
        if ( value instanceof byte[] ) {
            return buildTypedValueFromObject( value, Types.BINARY );
        }
        if ( value instanceof BigInteger ) {
            //requires conversion
            return buildTypedValueFromObject( value, Types.BIGINT );
        }
        if ( value instanceof Date ) {
            return buildTypedValueFromObject( value, Types.DATE );
        }
        if ( value instanceof Time ) {
            return buildTypedValueFromObject( value, Types.TIME );
        }
        if ( value instanceof Timestamp ) {
            return buildTypedValueFromObject( value, Types.TIMESTAMP );
        }
        if ( value instanceof NClob ) {
            // extends NClob
            return buildTypedValueFromObject( value, Types.NCLOB );
        }
        if ( value instanceof Clob ) {
            return buildTypedValueFromObject( value, Types.CLOB );
        }
        if ( value instanceof Blob ) {
            return buildTypedValueFromObject( value, Types.BLOB );
        }
        if ( value instanceof Array ) {
            return buildTypedValueFromObject( value, Types.ARRAY );
        }
        if ( value instanceof Struct ) {
            return buildTypedValueFromObject( value, Types.STRUCT );
        }
        if ( value instanceof Ref ) {
            return buildTypedValueFromObject( value, Types.REF );
        }
        if ( value instanceof URL ) {
            return buildTypedValueFromObject( value, Types.DATALINK );
        }
        if ( value instanceof RowId ) {
            return buildTypedValueFromObject( value, Types.ROWID );
        }
        if ( value instanceof SQLXML ) {
            return buildTypedValueFromObject( value, Types.SQLXML );
        }
        if ( value instanceof Calendar ) {
            // requires conversion
            return buildTypedValueFromObject( value, Types.TIMESTAMP );
        }
        if ( value instanceof java.util.Date ) {
            // requires conversion
            return buildTypedValueFromObject( value, Types.TIMESTAMP );
        }
        if ( value instanceof LocalDate ) {
            // requires conversion
            return buildTypedValueFromObject( value, Types.DATE );
        }
        if ( value instanceof LocalTime ) {
            // requires conversion
            return buildTypedValueFromObject( value, Types.TIME );
        }
        if ( value instanceof LocalDateTime ) {
            //requires conversion
            return buildTypedValueFromObject( value, Types.TIMESTAMP );
        }
        if ( value instanceof OffsetTime ) {
            return buildTypedValueFromObject( value, Types.TIME_WITH_TIMEZONE );
        }
        if ( value instanceof OffsetDateTime ) {
            return buildTypedValueFromObject( value, Types.TIMESTAMP_WITH_TIMEZONE );
        }
        return buildTypedValueFromObject( value, Types.JAVA_OBJECT );
    }


    public static TypedValue buildTypedValueFromObject( Object value, int targetSqlType ) throws ParseException, SQLFeatureNotSupportedException {
        if ( value instanceof String ) {
            return buildTypedValueFromString( (String) value, targetSqlType );
        }
        if ( value instanceof BigDecimal ) {
            return buildTypedValueFromBigDecimal( (BigDecimal) value, targetSqlType );
        }
        if ( value instanceof Boolean ) {
            return buildTypedValueFromBoolean( (Boolean) value, targetSqlType );
        }
        if ( value instanceof Byte ) {
            return buildTypedValueFromByte( (Byte) value, targetSqlType );
        }
        if ( value instanceof Short ) {
            return buildTypedValueFromShort( (Short) value, targetSqlType );
        }
        if ( value instanceof Integer ) {
            return buildTypedValueFromInteger( (Integer) value, targetSqlType );
        }
        if ( value instanceof Long ) {
            return buildTypedValueFromLong( (Long) value, targetSqlType );
        }
        if ( value instanceof Float ) {
            return buildTypedValueFromFloat( (Float) value, targetSqlType );
        }
        if ( value instanceof Double ) {
            return buildTypedValueFromDouble( (Double) value, targetSqlType );
        }
        if ( value instanceof byte[] ) {
            return buildTypedValueFromBytes( (byte[]) value, targetSqlType );
        }
        if ( value instanceof BigInteger ) {
            //requires conversion
            return buildTypedValueFromBigInteger( (BigInteger) value, targetSqlType );
        }
        if ( value instanceof Date ) {
            return buildTypedValueFromDate( (Date) value, targetSqlType );
        }
        if ( value instanceof Time ) {
            return buildTypedValueFromTime( (Time) value, targetSqlType );
        }
        if ( value instanceof Timestamp ) {
            return buildTypedValueFromTimestamp( (Timestamp) value, targetSqlType );
        }
        if ( value instanceof NClob ) {
            // extends NClob
            return buildTypedValueFromNClob( (NClob) value, targetSqlType );
        }
        if ( value instanceof Clob ) {
            return buildTypedValueFromClob( (Clob) value, targetSqlType );
        }
        if ( value instanceof Blob ) {
            return buildTypedValueFromBlob( (Blob) value, targetSqlType );
        }
        if ( value instanceof Array ) {
            return buildTypedValueFromArray( (Array) value, targetSqlType );
        }
        if ( value instanceof Struct ) {
            return buildTypedValueFromStruct( (Struct) value, targetSqlType );
        }
        if ( value instanceof Ref ) {
            return buildTypedValueFromRef( (Ref) value, targetSqlType );
        }
        if ( value instanceof URL ) {
            return buildTypedValueFromURL( (URL) value, targetSqlType );
        }
        if ( value instanceof RowId ) {
            return buildTypedValueFromRowId( (RowId) value, targetSqlType );
        }
        if ( value instanceof SQLXML ) {
            return buildTypedValueFromSQXML( (SQLXML) value, targetSqlType );
        }
        if ( value instanceof Calendar ) {
            // requires conversion
            return buildTypedValueFromCalendar( (Calendar) value, targetSqlType );
        }
        if ( value instanceof java.util.Date ) {
            // requires conversion
            return buildTypedValueFromDate( (java.util.Date) value, targetSqlType );
        }
        if ( value instanceof LocalDate ) {
            // requires conversion
            return buildTypedValueFromLocalDate( (LocalDate) value, targetSqlType );
        }
        if ( value instanceof LocalTime ) {
            // requires conversion
            return buildTypedValueFromLocalTime( (LocalTime) value, targetSqlType );
        }
        if ( value instanceof LocalDateTime ) {
            //requires conversion
            return buildTypedValueFromLocalDateTime( (LocalDateTime) value, targetSqlType );
        }
        if ( value instanceof OffsetTime ) {
            return buildTypedValueFromOffsetTime( (OffsetTime) value, targetSqlType );
        }
        if ( value instanceof OffsetDateTime ) {
            return buildTypedValueFromOffsetDateTime( (OffsetDateTime) value, targetSqlType );
        }
        return buildTypedValueFromJavaObject(value, targetSqlType);
    }


    private static TypedValue buildTypedValueFromJavaObject( Object value, int targetSqlType ) throws ParseException {
        if (targetSqlType != Types.JAVA_OBJECT) {
            throw new ParseException( "Can't parse Object as type " + targetSqlType, 0 );
        }
        return TypedValue.fromJavaObject(value);
    }


    private static TypedValue buildTypedValueFromOffsetDateTime( OffsetDateTime value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return TypedValue.fromTimestamp( getTimestampFromOffsetDateTime( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse OffsetDateTime as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromOffsetTime( OffsetTime value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIME_WITH_TIMEZONE:
                return TypedValue.fromTime( getTimeFromOffsetTime( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse OffsetTime as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromLocalDateTime( LocalDateTime value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( Time.valueOf( value.toLocalTime() ) );
            case Types.DATE:
                return TypedValue.fromDate( Date.valueOf( value.toLocalDate() ) );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( Timestamp.valueOf( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse LocalDateTime as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromLocalTime( LocalTime value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( Time.valueOf( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse LocalTime as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromLocalDate( LocalDate value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.DATE:
                return TypedValue.fromDate( Date.valueOf( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse LocalTime as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromDate( java.util.Date value, int targetSqlType ) throws ParseException, SQLFeatureNotSupportedException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( new Time( value.getTime() ) );
            case Types.DATE:
                return TypedValue.fromDate( new Date( value.getTime() ) );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( new Timestamp( value.getTime() ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
            case Types.ARRAY:
                throw new SQLFeatureNotSupportedException( "Parsing of Date as an Array is not supported" );
        }
        throw new ParseException( "Can't parse Date as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromCalendar( Calendar value, int targetSqlType ) throws ParseException, SQLFeatureNotSupportedException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( new Time( value.getTimeInMillis() ) );
            case Types.DATE:
                return TypedValue.fromDate( new Date( value.getTimeInMillis() ) );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( new Timestamp( value.getTimeInMillis() ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
            case Types.ARRAY:
                throw new SQLFeatureNotSupportedException( "Parsing of Calendar as an Array is not supported" );
        }
        throw new ParseException( "Can't parse Calendar as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromSQXML( SQLXML value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.SQLXML ) {
            throw new ParseException( "Can't parse SQLXML as type " + targetSqlType, 0 );
        }
        return TypedValue.fromSQLXML( value );
    }


    private static TypedValue buildTypedValueFromRowId( RowId value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.ROWID ) {
            throw new ParseException( "Can't parse RowId as type " + targetSqlType, 0 );
        }
        return TypedValue.fromRowId( value );
    }


    private static TypedValue buildTypedValueFromURL( URL value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.DATALINK ) {
            throw new ParseException( "Can't parse URL as type " + targetSqlType, 0 );
        }
        return TypedValue.fromUrl( value );
    }


    private static TypedValue buildTypedValueFromRef( Ref value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.REF ) {
            throw new ParseException( "Can't parse Ref as type " + targetSqlType, 0 );
        }
        return TypedValue.fromRef( value );
    }


    private static TypedValue buildTypedValueFromStruct( Struct value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.STRUCT ) {
            throw new ParseException( "Can't parse Struct as type " + targetSqlType, 0 );
        }
        return TypedValue.fromStruct( value );
    }


    private static TypedValue buildTypedValueFromArray( Array value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.ARRAY ) {
            throw new ParseException( "Can't parse Array as type " + targetSqlType, 0 );
        }
        return TypedValue.fromArray( value );
    }


    private static TypedValue buildTypedValueFromBlob( Blob value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.BLOB ) {
            throw new ParseException( "Can't parse Blob as type " + targetSqlType, 0 );
        }
        return TypedValue.fromBlob( value );
    }


    private static TypedValue buildTypedValueFromClob( Clob value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.CLOB ) {
            throw new ParseException( "Can't parse Clob as type " + targetSqlType, 0 );
        }
        return TypedValue.fromClob( value );
    }


    private static TypedValue buildTypedValueFromNClob( NClob value, int targetSqlType ) throws ParseException {
        if ( targetSqlType != Types.NCLOB ) {
            throw new ParseException( "Can't parse NClob as type " + targetSqlType, 0 );
        }
        return TypedValue.fromNClob( value );
    }


    private static TypedValue buildTypedValueFromTimestamp( Timestamp value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( new Time( value.getTime() ) );
            case Types.DATE:
                return TypedValue.fromDate( new Date( value.getTime() ) );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( value );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Timestamp as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromTime( Time value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TIME:
                return TypedValue.fromTime( value );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( new Timestamp( value.getTime() ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( SQL_TIME_FORMAT.format( value ) );
        }
        throw new ParseException( "Can't parse Time as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromDate( Date value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.DATE:
                return TypedValue.fromDate( value );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( new Timestamp( value.getTime() ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( SQL_DATE_FORMAT.format( value ) );
        }
        throw new ParseException( "Can't parse Date as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromBigInteger( BigInteger value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.BIGINT:
                return TypedValue.fromLong( value.longValue() );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse BigInteger as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromBytes( byte[] value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return TypedValue.fromBytes( value );
        }
        throw new ParseException( "Can't parse byte[] as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromDouble( Double value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value.shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( value.intValue() );
            case Types.BIGINT:
                return TypedValue.fromLong( value.longValue() );
            case Types.REAL:
                return TypedValue.fromFloat( value.floatValue() );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Double as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromFloat( Float value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value.shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( value.intValue() );
            case Types.BIGINT:
                return TypedValue.fromLong( value.longValue() );
            case Types.REAL:
                return TypedValue.fromFloat( value );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Float as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromLong( Long value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value.shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( value.intValue() );
            case Types.BIGINT:
                return TypedValue.fromLong( value );
            case Types.REAL:
                return TypedValue.fromFloat( value );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Long as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromInteger( Integer value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value.shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( value );
            case Types.BIGINT:
                return TypedValue.fromLong( value );
            case Types.REAL:
                return TypedValue.fromFloat( value );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Integer as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromShort( Short value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value );
            case Types.INTEGER:
                return TypedValue.fromInt( value );
            case Types.BIGINT:
                return TypedValue.fromLong( value );
            case Types.REAL:
                return TypedValue.fromFloat( value );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Short as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromByte( Byte value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value );
            case Types.SMALLINT:
                return TypedValue.fromShort( value );
            case Types.INTEGER:
                return TypedValue.fromInt( value );
            case Types.BIGINT:
                return TypedValue.fromLong( value );
            case Types.REAL:
                return TypedValue.fromFloat( value );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( getBooleanFromNumber( value ) );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse Byte as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromBoolean( Boolean value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( getNumberFromBoolean( value ).byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( getNumberFromBoolean( value ).shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( getNumberFromBoolean( value ).intValue() );
            case Types.BIGINT:
                return TypedValue.fromLong( getNumberFromBoolean( value ).longValue() );
            case Types.REAL:
                return TypedValue.fromFloat( getNumberFromBoolean( value ).floatValue() );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( getNumberFromBoolean( value ).doubleValue() );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( getNumberFromBoolean( value ).intValue() ) );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( value );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( getOneZeroStringFromBoolean( value ) );
        }
        throw new ParseException( "Can't parse Boolean as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromBigDecimal( BigDecimal value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( value.byteValue() );
            case Types.SMALLINT:
                return TypedValue.fromShort( value.shortValue() );
            case Types.INTEGER:
                return TypedValue.fromInt( value.intValue() );
            case Types.BIGINT:
                return TypedValue.fromLong( value.longValue() );
            case Types.REAL:
                return TypedValue.fromFloat( value.floatValue() );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( value.doubleValue() );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( value );
            case Types.BIT:
            case Types.BOOLEAN:
                return TypedValue.fromBoolean( value.intValue() != 0 );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return TypedValue.fromString( value.toString() );
        }
        throw new ParseException( "Can't parse BigDecimal as type " + targetSqlType, 0 );
    }


    private static TypedValue buildTypedValueFromString( String value, int targetSqlType ) throws ParseException {
        switch ( targetSqlType ) {
            case Types.TINYINT:
                return TypedValue.fromByte( Byte.parseByte( value ) );
            case Types.SMALLINT:
                return TypedValue.fromShort( Short.parseShort( value ) );
            case Types.INTEGER:
                return TypedValue.fromInt( Integer.parseInt( value ) );
            case Types.BIGINT:
                return TypedValue.fromLong( Long.parseLong( value ) );
            case Types.REAL:
                return TypedValue.fromFloat( Float.parseFloat( value ) );
            case Types.FLOAT:
            case Types.DOUBLE:
                // according to jdbc spec double should be used for jdbc float
                return TypedValue.fromDouble( Double.parseDouble( value ) );
            case Types.DECIMAL:
            case Types.NUMERIC:
                return TypedValue.fromBigDecimal( new BigDecimal( value ) );
            case Types.BIT:
            case Types.BOOLEAN:
                if ( value.equals( "0" ) ) {
                    return TypedValue.fromBoolean( false );
                }
                if ( value.equals( "1" ) ) {
                    return TypedValue.fromBoolean( true );
                }
                if ( value.equalsIgnoreCase( "true" ) ) {
                    return TypedValue.fromBoolean( true );
                }
                return TypedValue.fromBoolean( false );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return TypedValue.fromString( value );
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return TypedValue.fromBytes( value.getBytes( StandardCharsets.UTF_8 ) );
            case Types.DATE:
                return TypedValue.fromDate( dateFromString( value ) );
            case Types.TIME:
                return TypedValue.fromTime( timeFromString( value ) );
            case Types.TIMESTAMP:
                return TypedValue.fromTimestamp( Timestamp.valueOf( value ) );
        }
        throw new ParseException( "Can't parse String as type " + targetSqlType, 0 );
    }


    public static TypedValue fromObject( Object value, int targetSqlType, int scaleOrLength ) throws NotImplementedException {
        throw new NotImplementedException( "Not yet implemented..." );
    }

}

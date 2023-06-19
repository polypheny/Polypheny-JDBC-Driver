package org.polypheny.jdbc.types;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
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


    @Override
    public boolean isSqlNull() throws SQLException {
        return jdbcType == Types.NULL;
    }


    @Override
    public String asString() throws SQLException {
        if ( isSqlNull() ) {
            return null;
        }
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
        //TODO TH: implement this
        throw new SQLException( "Value retrieval as bytes is not supported yet." );
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
        return null;
    }


    @Override
    public Clob asClob() throws SQLException {
        return null;
    }


    @Override
    public Array asArray() throws SQLException {
        return null;
    }


    @Override
    public Struct asStruct() throws SQLException {
        return null;
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

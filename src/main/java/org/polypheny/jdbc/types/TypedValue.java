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
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Struct;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Map;
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
        return null;
    }


    @Override
    public boolean asBoolean() throws SQLException {
        return false;
    }


    @Override
    public byte asByte() throws SQLException {
        return 0;
    }


    @Override
    public short asShort() throws SQLException {
        return 0;
    }


    @Override
    public int asInt() throws SQLException {
        return 0;
    }


    @Override
    public long asLong() throws SQLException {
        return 0;
    }


    @Override
    public float asFloat() throws SQLException {
        return 0;
    }


    @Override
    public double asDouble() throws SQLException {
        return 0;
    }


    @Override
    public BigDecimal asBigDecimal( int scale ) throws SQLException {
        return null;
    }


    @Override
    public BigDecimal asBigDecimal() throws SQLException {
        return null;
    }


    @Override
    public byte[] asBytes() throws SQLException {
        return new byte[0];
    }


    @Override
    public InputStream asAsciiStream() throws SQLException {
        return null;
    }


    @Override
    public InputStream asUnicodeStream() throws SQLException {
        return null;
    }


    @Override
    public InputStream asBinaryStream() throws SQLException {
        return null;
    }


    @Override
    public Object asObject() throws SQLException {
        return null;
    }


    @Override
    public Object asObject( Map<String, Class<?>> map ) throws SQLException {
        return null;
    }


    @Override
    public Reader asCharacterStream() throws SQLException {
        return null;
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
        return null;
    }


    @Override
    public Timestamp asTimestamp( Calendar calendar ) throws SQLException {
        return null;
    }


    @Override
    public URL asUrl() throws SQLException {
        return null;
    }


    @Override
    public NClob asNClob() throws SQLException {
        return null;
    }


    @Override
    public SQLXML asSQLXML() throws SQLException {
        return null;
    }


    @Override
    public String asNString() throws SQLException {
        return null;
    }


    @Override
    public Reader asNCharacterStream() throws SQLException {
        return null;
    }


    @Override
    public <T> T asObject( Class<T> type ) throws SQLException {
        return null;
    }

}

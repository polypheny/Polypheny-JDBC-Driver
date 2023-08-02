package org.polypheny.jdbc.types;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;
import org.polypheny.jdbc.ProtoInterfaceServiceException;

public interface Convertible {

    boolean isSqlNull();

    boolean isNull();


    String asString() throws SQLException;


    boolean asBoolean() throws SQLException;


    byte asByte() throws SQLException;


    short asShort() throws SQLException;


    int asInt() throws SQLException;


    long asLong() throws SQLException;


    float asFloat() throws SQLException;


    double asDouble() throws SQLException;


    BigDecimal asBigDecimal() throws SQLException;


    BigDecimal asBigDecimal(int scale) throws SQLException;


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

    Date asDate(Calendar calendar) throws SQLException;

    Time asTime() throws SQLException;

    Time asTime(Calendar calendar) throws SQLException;

    Timestamp asTimestamp() throws SQLException;

    Timestamp asTimestamp(Calendar calendar) throws SQLException;

    Ref asRef() throws SQLException;

    RowId asRowId() throws SQLException;

    URL asUrl() throws SQLException;


    NClob asNClob() throws SQLException;


    SQLXML asSQLXML() throws SQLException;


    String asNString() throws SQLException;


    Reader asNCharacterStream() throws SQLException;

    Object asObject(Map<String, Class<?>> map) throws SQLException;

    Object asObject( Calendar calendar ) throws SQLException;

    <T> T asObject(Class<T> aClass) throws SQLFeatureNotSupportedException, ProtoInterfaceServiceException;
}

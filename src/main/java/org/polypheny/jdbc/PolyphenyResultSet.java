package org.polypheny.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.proto.Value;
import org.polypheny.jdbc.utils.ProtoValueDeserializer;

public class PolyphenyResultSet implements ResultSet {

    public PolyphenyResultSet( Frame frame ) {
        List<Row> rows = frame.getRowsList();
        List<List<Value>> valueRows = rows.stream().map(r -> new ArrayList<>( r.getValuesList() ) ).collect( Collectors.toList());


    }


    @Override
    public boolean next() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void close() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean wasNull() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getString( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean getBoolean( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public byte getByte( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public short getShort( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getInt( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public long getLong( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public float getFloat( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public double getDouble( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public BigDecimal getBigDecimal( int i, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public byte[] getBytes( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Date getDate( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Time getTime( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Timestamp getTimestamp( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getAsciiStream( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getUnicodeStream( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getBinaryStream( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getString( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean getBoolean( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public byte getByte( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public short getShort( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getInt( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public long getLong( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public float getFloat( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public double getDouble( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public BigDecimal getBigDecimal( String s, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public byte[] getBytes( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Date getDate( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Time getTime( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Timestamp getTimestamp( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getAsciiStream( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getUnicodeStream( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getBinaryStream( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void clearWarnings() throws SQLException {
// saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getCursorName() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Object getObject( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Object getObject( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int findColumn( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Reader getCharacterStream( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Reader getCharacterStream( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public BigDecimal getBigDecimal( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public BigDecimal getBigDecimal( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isBeforeFirst() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isAfterLast() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isFirst() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isLast() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void beforeFirst() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void afterLast() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean first() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean last() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean absolute( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean relative( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean previous() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void setFetchDirection( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getFetchDirection() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void setFetchSize( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getFetchSize() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getType() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getConcurrency() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean rowUpdated() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean rowInserted() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean rowDeleted() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNull( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBoolean( int i, boolean b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateByte( int i, byte b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateShort( int i, short i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateInt( int i, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateLong( int i, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateFloat( int i, float v ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateDouble( int i, double v ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBigDecimal( int i, BigDecimal bigDecimal ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateString( int i, String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBytes( int i, byte[] bytes ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateDate( int i, Date date ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateTime( int i, Time time ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateTimestamp( int i, Timestamp timestamp ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( int i, Reader reader, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateObject( int i, Object o, int i1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateObject( int i, Object o ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNull( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBoolean( String s, boolean b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateByte( String s, byte b ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateShort( String s, short i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateInt( String s, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateLong( String s, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateFloat( String s, float v ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateDouble( String s, double v ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBigDecimal( String s, BigDecimal bigDecimal ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateString( String s, String s1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBytes( String s, byte[] bytes ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateDate( String s, Date date ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateTime( String s, Time time ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateTimestamp( String s, Timestamp timestamp ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( String s, Reader reader, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateObject( String s, Object o, int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateObject( String s, Object o ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void insertRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void deleteRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void refreshRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void cancelRowUpdates() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void moveToInsertRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void moveToCurrentRow() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Statement getStatement() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Object getObject( int i, Map<String, Class<?>> map ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Ref getRef( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Blob getBlob( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Clob getClob( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Array getArray( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Object getObject( String s, Map<String, Class<?>> map ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Ref getRef( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Blob getBlob( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Clob getClob( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Array getArray( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Date getDate( int i, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Date getDate( String s, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Time getTime( int i, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Time getTime( String s, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Timestamp getTimestamp( int i, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Timestamp getTimestamp( String s, Calendar calendar ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public URL getURL( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public URL getURL( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateRef( int i, Ref ref ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateRef( String s, Ref ref ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( int i, Blob blob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( String s, Blob blob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( int i, Clob clob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( String s, Clob clob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateArray( int i, Array array ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateArray( String s, Array array ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public RowId getRowId( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public RowId getRowId( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateRowId( int i, RowId rowId ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateRowId( String s, RowId rowId ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public int getHoldability() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isClosed() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNString( int i, String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNString( String s, String s1 ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( int i, NClob nClob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( String s, NClob nClob ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public NClob getNClob( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public NClob getNClob( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public SQLXML getSQLXML( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public SQLXML getSQLXML( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateSQLXML( int i, SQLXML sqlxml ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateSQLXML( String s, SQLXML sqlxml ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getNString( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public String getNString( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Reader getNCharacterStream( int i ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Reader getNCharacterStream( String s ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNCharacterStream( int i, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNCharacterStream( String s, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream, long l ) throws SQLException {
// saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( int i, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( String s, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( int i, InputStream inputStream, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( String s, InputStream inputStream, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( int i, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( String s, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( int i, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( String s, Reader reader, long l ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNCharacterStream( int i, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNCharacterStream( String s, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( int i, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateCharacterStream( String s, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( int i, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateBlob( String s, InputStream inputStream ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( int i, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateClob( String s, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( int i, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public void updateNClob( String s, Reader reader ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public <T> T getObject( int i, Class<T> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public <T> T getObject( String s, Class<T> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public <T> T unwrap( Class<T> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public boolean isWrapperFor( Class<?> aClass ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }

}

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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.types.TypedValue;

public class PolyphenyForwardResultSet implements ResultSet {
    private PolyphenyStatement statement;

    private final PolyphenyResultSetMetadata metadata;
    private final ForwardOnlyScroller resultScroller;
    private TypedValue lastRead;
    private boolean isClosed;
    private boolean isClosedOnCompletion;

    ResultSetProperties properties;


    public PolyphenyForwardResultSet(
            PolyphenyStatement statement,
            Frame frame,
            ResultSetProperties properties
    ) throws SQLException {
        if ( frame.getResultCase() != ResultCase.RELATIONAL_FRAME ) {
            throw new SQLException( "Invalid frame type " + frame.getResultCase().name() );
        }
        this.statement = statement;
        this.metadata = new PolyphenyResultSetMetadata( frame.getRelationalFrame().getColumnMetaList() );
        this.resultScroller = new ForwardOnlyScroller( frame, getClient(), statement.getStatementId(), properties);
        this.properties = properties;
        this.lastRead = null;
        this.isClosed = false;
    }


    private TypedValue accessValue( int column ) throws SQLException {
        try {
            lastRead = resultScroller.current().get( column - 1 );
            return lastRead;
        } catch ( IndexOutOfBoundsException e ) {
            throw new SQLException( "Column index out of bounds." );
        }
    }


    private void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "This operation cannot be applied to a closed result set." );
        }
    }


    @Override
    public boolean next() throws SQLException {
        throwIfClosed();
        try {
            return resultScroller.next();
        } catch ( InterruptedException e ) {
            throw new SQLException( e );
        }
    }


    private ProtoInterfaceClient getClient() {
        return statement.getClient();
    }


    @Override
    public void close() throws SQLException {
    }


    @Override
    public boolean wasNull() throws SQLException {
        throwIfClosed();
        return lastRead.isSqlNull();
    }


    @Override
    public String getString( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asString();
    }


    @Override
    public boolean getBoolean( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asBoolean();
    }


    @Override
    public byte getByte( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asByte();
    }


    @Override
    public short getShort( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asShort();
    }


    @Override
    public int getInt( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asInt();
    }


    @Override
    public long getLong( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asLong();
    }


    @Override
    public float getFloat( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asFloat();
    }


    @Override
    public double getDouble( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asDouble();
    }


    @Override
    public BigDecimal getBigDecimal( int columnIndex, int scale ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asBigDecimal( scale );
    }


    @Override
    public byte[] getBytes( int columnIndex ) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Date getDate( int columnIndex ) throws SQLException {
        throwIfClosed();
        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asDate(  );

        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Time getTime( int columnIndex ) throws SQLException {
        throwIfClosed();

        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asTime(  );

        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public Timestamp getTimestamp( int columnIndex ) throws SQLException {
        throwIfClosed();
        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asTimestamp(  );

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException( "Feature " + methodName + " not implemented" );
    }


    @Override
    public InputStream getAsciiStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asAsciiStream();
    }


    @Override
    public InputStream getUnicodeStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asUnicodeStream();
    }


    @Override
    public InputStream getBinaryStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asBinaryStream();
    }


    @Override
    public String getString( String columnLabel ) throws SQLException {
        return getString( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public boolean getBoolean( String columnLabel ) throws SQLException {
        return getBoolean( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public byte getByte( String columnLabel ) throws SQLException {
        return getByte( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public short getShort( String columnLabel ) throws SQLException {
        return getShort( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public int getInt( String columnLabel ) throws SQLException {
        return getInt( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public long getLong( String columnLabel ) throws SQLException {
        return getLong( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public float getFloat( String columnLabel ) throws SQLException {
        return getFloat( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public double getDouble( String columnLabel ) throws SQLException {
        return getDouble( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public BigDecimal getBigDecimal( String columnLabel, int scale ) throws SQLException {
        return getBigDecimal( metadata.getIndexFromLabel( columnLabel ), scale );
    }


    @Override
    public byte[] getBytes( String columnLabel ) throws SQLException {
        return getBytes( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public Date getDate( String columnLabel ) throws SQLException {
        return getDate( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public Time getTime( String columnLabel ) throws SQLException {
        return getTime( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public Timestamp getTimestamp( String columnLabel ) throws SQLException {
        return getTimestamp( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getAsciiStream( String columnLabel ) throws SQLException {
        return getAsciiStream( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getUnicodeStream( String columnLabel ) throws SQLException {
        return getUnicodeStream( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getBinaryStream( String columnLabel ) throws SQLException {
        return getBinaryStream( metadata.getIndexFromLabel( columnLabel ) );
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
        return metadata;
    }


    @Override
    public Object getObject( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asObject();
    }


    @Override
    public Object getObject( String columnLabel ) throws SQLException {
        return getObject( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public int findColumn( String columnLabel ) throws SQLException {
        throwIfClosed();
        return metadata.getIndexFromLabel( columnLabel );
    }


    @Override
    public Reader getCharacterStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asCharacterStream();
    }


    @Override
    public Reader getCharacterStream( String columnLabel ) throws SQLException {
        return getCharacterStream( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public BigDecimal getBigDecimal( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex ).asBigDecimal();
    }


    @Override
    public BigDecimal getBigDecimal( String columnLabel ) throws SQLException {
        return getBigDecimal( metadata.getIndexFromLabel( columnLabel ) );
    }


    @Override
    public boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
    }


    @Override
    public boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
    }


    @Override
    public boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
    }


    @Override
    public boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
    }


    @Override
    public void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
    }


    @Override
    public void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException( "This feature is not supported." );
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
        return resultScroller.getRow();
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
    public void setFetchDirection( int fetchDirection ) throws SQLException {
        throwIfClosed();
        if ( properties.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY && fetchDirection != ResultSet.FETCH_FORWARD ) {
            throw new SQLException( "Illegal fetch direction for resultset of TYPE_FORWARD_ONLY." );
        }
        properties.setFetchDirection( fetchDirection );
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return properties.getFetchDirection();
    }


    @Override
    public void setFetchSize( int fetchSize ) throws SQLException {
        throwIfClosed();
        if ( fetchSize < 0 ) {
            throw new SQLException( "Illegal value for fetch size. fetchSize >= 0 must hold." );
        }
        properties.setFetchSize( fetchSize );
    }


    @Override
    public int getFetchSize() throws SQLException {
        throwIfClosed();
        return properties.getFetchSize();
    }


    @Override
    public int getType() throws SQLException {
        throwIfClosed();
        return properties.getResultSetType();
    }


    @Override
    public int getConcurrency() throws SQLException {
        throwIfClosed();
        return properties.getResultSetConcurrency();
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
        throwIfClosed();
        return statement;
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
        throwIfClosed();
        return properties.getResultSetHoldability();
    }


    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
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

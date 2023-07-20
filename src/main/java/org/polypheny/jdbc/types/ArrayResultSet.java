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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.polypheny.jdbc.meta.PolyphenyResultSetMetadata;

public class ArrayResultSet<T> implements ResultSet {

    ArrayList<ArrayList<T>> data;
    ArrayList<T> current;
    T lastRead;
    PolyphenyResultSetMetadata metadata;

    int currentIndex = -1;
    boolean isClosed = false;


    public ArrayResultSet( ArrayList<ArrayList<T>> data, PolyphenyResultSetMetadata metadata ) {
        this.data = data;
        this.metadata = metadata;
        this.current = null;
        this.lastRead = null;
    }


    private void throwIfClosed() throws SQLException {
        if ( isClosed ) {
            throw new SQLException( "This operation cannot be applied to a closed result set." );
        }
    }


    private Object accessValue( int column ) throws SQLException {
        try {
            lastRead = current.get( column - 1 );
            return lastRead;
        } catch ( IndexOutOfBoundsException e ) {
            throw new SQLException( "Column index out of bounds." );
        }
    }


    @Override
    public boolean next() throws SQLException {
        currentIndex++;
        if ( currentIndex == data.size() ) {
            current = null;
            return false;
        }
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public void close() throws SQLException {
    }


    @Override
    public boolean wasNull() throws SQLException {
        throwIfClosed();
        return current == null;
    }


    @Override
    public String getString( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof String ) {
            return (String) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public boolean getBoolean( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Boolean ) {
            return (Boolean) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public byte getByte( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Byte ) {
            return (Byte) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public short getShort( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Short ) {
            return (Short) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public int getInt( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Integer ) {
            return (Integer) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public long getLong( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Long ) {
            return (Long) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public float getFloat( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Float ) {
            return (Float) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public double getDouble( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Double ) {
            return (Double) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public BigDecimal getBigDecimal( int columnIndex, int scale ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof BigDecimal ) {
            return (BigDecimal) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public byte[] getBytes( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Byte[] ) {
            return ArrayUtils.toPrimitive( (Byte[]) lastRead );
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Date getDate( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Date ) {
            return (Date) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Time getTime( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Time ) {
            return (Time) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Timestamp getTimestamp( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Timestamp ) {
            return (Timestamp) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public InputStream getAsciiStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof InputStream ) {
            return (InputStream) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public InputStream getUnicodeStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof InputStream ) {
            return (InputStream) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public InputStream getBinaryStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof InputStream ) {
            return (InputStream) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public String getString( String columnLabel ) throws SQLException {
        return getString( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public boolean getBoolean( String columnLabel ) throws SQLException {
        return getBoolean( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public byte getByte( String columnLabel ) throws SQLException {
        return getByte( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public short getShort( String columnLabel ) throws SQLException {
        return getShort( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public int getInt( String columnLabel ) throws SQLException {
        return getInt( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public long getLong( String columnLabel ) throws SQLException {
        return getLong( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public float getFloat( String columnLabel ) throws SQLException {
        return getFloat( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public double getDouble( String columnLabel ) throws SQLException {
        return getDouble( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public BigDecimal getBigDecimal( String columnLabel, int scale ) throws SQLException {
        return getBigDecimal( metadata.getColumnIndexFromLabel( columnLabel ), scale );
    }


    @Override
    public byte[] getBytes( String columnLabel ) throws SQLException {
        return getBytes( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public Date getDate( String columnLabel ) throws SQLException {
        return getDate( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public Time getTime( String columnLabel ) throws SQLException {
        return getTime( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public Timestamp getTimestamp( String columnLabel ) throws SQLException {
        return getTimestamp( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getAsciiStream( String columnLabel ) throws SQLException {
        return getAsciiStream( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getUnicodeStream( String columnLabel ) throws SQLException {
        return getUnicodeStream( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public InputStream getBinaryStream( String columnLabel ) throws SQLException {
        return getBinaryStream( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public void clearWarnings() throws SQLException {
    }


    @Override
    public String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException( "Feature not supported" );
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }


    @Override
    public Object getObject( int columnIndex ) throws SQLException {
        throwIfClosed();
        return accessValue( columnIndex );
    }


    @Override
    public Object getObject( String columnLabel ) throws SQLException {
        return getObject( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public int findColumn( String columnLabel ) throws SQLException {
        throwIfClosed();
        return metadata.getColumnIndexFromLabel( columnLabel );
    }


    @Override
    public Reader getCharacterStream( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof Reader ) {
            return (Reader) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Reader getCharacterStream( String columnLabel ) throws SQLException {
        throwIfClosed();
        if ( accessValue( metadata.getColumnIndexFromLabel( columnLabel ) ) instanceof Reader ) {
            return (Reader) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public BigDecimal getBigDecimal( int columnIndex ) throws SQLException {
        throwIfClosed();
        if ( accessValue( columnIndex ) instanceof BigDecimal ) {
            return (BigDecimal) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public BigDecimal getBigDecimal( String columnLabel ) throws SQLException {
        return getBigDecimal( metadata.getColumnIndexFromLabel( columnLabel ) );
    }


    @Override
    public boolean isBeforeFirst() throws SQLException {
        return currentIndex == -1;
    }


    @Override
    public boolean isAfterLast() throws SQLException {
        return currentIndex == data.size();
    }


    @Override
    public boolean isFirst() throws SQLException {
        return currentIndex == 0;
    }


    @Override
    public boolean isLast() throws SQLException {
        return currentIndex == data.size() - 1;
    }


    @Override
    public void beforeFirst() throws SQLException {
        currentIndex = -1;
        current = null;
    }


    @Override
    public void afterLast() throws SQLException {
        // This is not an off by one error. An index set to the length of an array is positioned after the last element.
        currentIndex = data.size();
        current = null;
    }


    @Override
    public boolean first() throws SQLException {
        currentIndex = 0;
        current = data.get( 0 );
        return true;
    }


    @Override
    public boolean last() throws SQLException {
        currentIndex = data.size() - 1;
        current = data.get(data.size() - 1);
        return true;
    }


    @Override
    public int getRow() throws SQLException {
        // JDBC starts enumeration at one
        return currentIndex + 1;
    }


    @Override
    public boolean absolute( int row ) throws SQLException {
        row--;
        if (row >= data.size()) {
            currentIndex = data.size();
            current = null;
            return false;
        }
        if (row == -1) {
            currentIndex = -1;
            current = null;
            return false;
        }
        if (row < 0) {
            return absolute( data.size() + row + 1);
        }
        currentIndex = row;
        current = data.get(row);
        return true;
    }


    @Override
    public boolean relative( int offset ) throws SQLException {
        int newCurrent = currentIndex + offset;
        if (newCurrent < 0) {
            currentIndex = -1;
            current = null;
            return false;
        }
        if (newCurrent >= data.size()) {
            // This is not an off by one error. An index set to the length of an array is positioned after the last element.
            currentIndex = data.size();
            current = null;
            return false;
        }
        currentIndex = newCurrent;
        current = data.get( currentIndex );
        return true;
    }


    @Override
    public boolean previous() throws SQLException {
        return relative( -1 );
    }


    @Override
    public void setFetchDirection( int fetchDirection ) throws SQLException {
        throwIfClosed();
        if (fetchDirection != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Illegal fetch direction for this result set");
        }
    }


    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }


    @Override
    public void setFetchSize( int fetchSize ) throws SQLException {
        if (fetchSize < 0) {
            throw new SQLException("Illegal value for fetchSize");
        }
    }


    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }


    @Override
    public int getType() throws SQLException {
        throwIfClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }


    @Override
    public int getConcurrency() throws SQLException {
        throwIfClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }


    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }


    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }


    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }


    @Override
    public void updateNull( int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBoolean( int i, boolean b ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateByte( int i, byte b ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateShort( int i, short i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateInt( int i, int i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateLong( int i, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateFloat( int i, float v ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateDouble( int i, double v ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBigDecimal( int i, BigDecimal bigDecimal ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateString( int i, String s ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBytes( int i, byte[] bytes ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateDate( int i, Date date ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateTime( int i, Time time ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateTimestamp( int i, Timestamp timestamp ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream, int i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream, int i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( int i, Reader reader, int i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateObject( int i, Object o, int i1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateObject( int i, Object o ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNull( String s ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBoolean( String s, boolean b ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateByte( String s, byte b ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateShort( String s, short i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateInt( String s, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateLong( String s, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateFloat( String s, float v ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateDouble( String s, double v ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBigDecimal( String s, BigDecimal bigDecimal ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateString( String s, String s1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBytes( String s, byte[] bytes ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateDate( String s, Date date ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateTime( String s, Time time ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateTimestamp( String s, Timestamp timestamp ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( String s, Reader reader, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateObject( String s, Object o, int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateObject( String s, Object o ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Statement getStatement() throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Object getObject( int i, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Ref getRef( int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Blob getBlob( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof Blob ) {
            return (Blob) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Clob getClob( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof Clob ) {
            return (Clob) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Array getArray( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof Array ) {
            return (Array) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Object getObject( String s, Map<String, Class<?>> map ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Ref getRef( String s ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Blob getBlob( String s ) throws SQLException {
        return getBlob( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public Clob getClob( String s ) throws SQLException {
        return getClob( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public Array getArray( String s ) throws SQLException {
        return getArray( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public Date getDate( int i, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Date getDate( String s, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Time getTime( int i, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Time getTime( String s, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Timestamp getTimestamp( int i, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public Timestamp getTimestamp( String s, Calendar calendar ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public URL getURL( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof URL ) {
            return (URL) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public URL getURL( String s ) throws SQLException {
        return getURL( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public void updateRef( int i, Ref ref ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateRef( String s, Ref ref ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( int i, Blob blob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( String s, Blob blob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( int i, Clob clob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( String s, Clob clob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateArray( int i, Array array ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateArray( String s, Array array ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public RowId getRowId( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof RowId ) {
            return (RowId) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public RowId getRowId( String s ) throws SQLException {
        return getRowId( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public void updateRowId( int i, RowId rowId ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateRowId( String s, RowId rowId ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public int getHoldability() throws SQLException {
        throwIfClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }


    @Override
    public boolean isClosed() throws SQLException {
        return isClosed;
    }


    @Override
    public void updateNString( int i, String s ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNString( String s, String s1 ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( int i, NClob nClob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( String s, NClob nClob ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public NClob getNClob( int i ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public NClob getNClob( String s ) throws SQLException {
        return getNClob( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public SQLXML getSQLXML( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof SQLXML ) {
            return (SQLXML) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public SQLXML getSQLXML( String s ) throws SQLException {
        return getSQLXML( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public void updateSQLXML( int i, SQLXML sqlxml ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateSQLXML( String s, SQLXML sqlxml ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public String getNString( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof String ) {
            return (String) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public String getNString( String s ) throws SQLException {
        return getNString( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public Reader getNCharacterStream( int i ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) instanceof Reader ) {
            return (Reader) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public Reader getNCharacterStream( String s ) throws SQLException {
        return getNCharacterStream( metadata.getColumnIndexFromLabel( s ) );
    }


    @Override
    public void updateNCharacterStream( int i, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNCharacterStream( String s, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( int i, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( String s, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( int i, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( String s, InputStream inputStream, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( int i, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( String s, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( int i, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( String s, Reader reader, long l ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNCharacterStream( int i, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNCharacterStream( String s, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( int i, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( int i, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( int i, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateAsciiStream( String s, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBinaryStream( String s, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateCharacterStream( String s, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( int i, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateBlob( String s, InputStream inputStream ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( int i, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateClob( String s, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( int i, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public void updateNClob( String s, Reader reader ) throws SQLException {
        throw new SQLFeatureNotSupportedException("Operation not supported");
    }


    @Override
    public <U> U getObject( int i, Class<U> aClass ) throws SQLException {
        throwIfClosed();
        if ( accessValue( i ) != null ) {
            return (U) lastRead;
        }
        throw new SQLException( "Conversion not supported" );
    }


    @Override
    public <T> T getObject( String s, Class<T> aClass ) throws SQLException {
        return getObject( metadata.getColumnIndexFromLabel( s ), aClass);
    }


    @Override
    public <T> T unwrap(Class<T> aClass) throws SQLException {
        if (aClass.isInstance(this)) {
            return aClass.cast(this);
        }
        throw new SQLException("Not a wrapper for " + aClass);
    }


    @Override
    public boolean isWrapperFor(Class<?> aClass) {
        return aClass.isInstance(this);

    }

}

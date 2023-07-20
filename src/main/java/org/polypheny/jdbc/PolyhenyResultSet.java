package org.polypheny.jdbc;

import org.polypheny.jdbc.meta.MetaScroller;
import org.polypheny.jdbc.meta.PolyphenyColumnMeta;
import org.polypheny.jdbc.meta.PolyphenyResultSetMetadata;
import org.polypheny.jdbc.properties.ResultSetProperties;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.Frame.ResultCase;
import org.polypheny.jdbc.types.TypedValue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;

public class PolyhenyResultSet implements ResultSet {

    private PolyphenyStatement statement;

    private PolyphenyResultSetMetadata metadata;
    private Scrollable<ArrayList<TypedValue>> resultScroller;
    private Class<BidirectionalScroller> bidirectionScrollerClass;
    private TypedValue lastRead;
    private boolean isClosed;
    private LinkedHashMap<Integer, TypedValue> rowUpdates;
    private boolean isInInsertMode;

    ResultSetProperties properties;


    public PolyhenyResultSet(
            PolyphenyStatement statement,
            Frame frame,
            ResultSetProperties properties
    ) throws SQLException {
        if (frame.getResultCase() != ResultCase.RELATIONAL_FRAME) {
            throw new SQLException("Invalid frame type " + frame.getResultCase().name());
        }
        this.statement = statement;
        this.metadata = new PolyphenyResultSetMetadata(frame.getRelationalFrame().getColumnMetaList());
        if (properties.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY) {
            this.resultScroller = new ForwardOnlyScroller(frame, getClient(), statement.getStatementId(), properties);
        } else {
            this.resultScroller = new BidirectionalScroller(frame, getClient(), statement.getStatementId(), properties);
        }
        this.bidirectionScrollerClass = BidirectionalScroller.class;
        this.properties = properties;
        this.lastRead = null;
        this.isClosed = false;
        this.isInInsertMode = false;
    }

    public PolyhenyResultSet(ArrayList<PolyphenyColumnMeta> columnMetas, ArrayList<ArrayList<TypedValue>> rows) {
        this.resultScroller = new MetaScroller<>(rows);
        this.metadata = new PolyphenyResultSetMetadata(columnMetas);
        this.statement = null;
        this.properties = ResultSetProperties.forMetaResultSet();
        this.lastRead = null;
        this.isClosed = false;
        this.isInInsertMode = false;
    }


    private TypedValue accessValue(int column) throws SQLException {
        if (!isInInsertMode) {
            try {
                lastRead = resultScroller.current().get(column - 1);
                return lastRead;
            } catch (IndexOutOfBoundsException e) {
                throw new SQLException("Column index out of bounds.");
            }
        }
        TypedValue value = rowUpdates.get(column);
        if (value == null) {
            throw new SQLException("Can't access unset colum");
        }
        return value;
    }


    private void throwIfClosed() throws SQLException {
        if (isClosed) {
            throw new SQLException("This operation cannot be applied to a closed result set.");
        }
    }

    private void throwIfColumnIndexOutOfBounds(int columnIndex) throws SQLException {
        if (columnIndex < 1) {
            throw new SQLException("Column index must be greater than 0");
        }
        if (columnIndex > metadata.getColumnCount()) {
            throw new SQLException("Column index out of bounds");
        }
    }

    private void throwIfReadOnly() throws SQLException {
        if (properties.isReadOnly()) {
            throw new SQLException("Modification of result sets in read only mode is not permitted");
        }
    }

    private void discardRowUpdates() {
        if (rowUpdates == null) {
            return;
        }
        rowUpdates = null;
    }

    private LinkedHashMap<Integer, TypedValue> getOrCreateRowUpdate() {
        if (rowUpdates == null) {
            rowUpdates = new LinkedHashMap<>();
        }
        return rowUpdates;
    }


    @Override
    public boolean next() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        try {
            return resultScroller.next();
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
    }


    private ProtoInterfaceClient getClient() {
        return statement.getClient();
    }

    private BidirectionalScroller getBidirectionalScrollerOrThrow() throws SQLException {
        if (resultScroller instanceof BidirectionalScroller) {
            return bidirectionScrollerClass.cast(resultScroller);
        }
        throw new SQLException("Illegal operation on resultset of type TYPE_FORWARD_ONLY");
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
    public String getString(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asString();
    }


    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBoolean();
    }


    @Override
    public byte getByte(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asByte();
    }


    @Override
    public short getShort(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asShort();
    }


    @Override
    public int getInt(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asInt();
    }


    @Override
    public long getLong(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asLong();
    }


    @Override
    public float getFloat(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asFloat();
    }


    @Override
    public double getDouble(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asDouble();
    }


    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBigDecimal(scale);
    }


    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBytes();
    }


    @Override
    public Date getDate(int columnIndex) throws SQLException {
        throwIfClosed();
        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asDate(  );

        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public Time getTime(int columnIndex) throws SQLException {
        throwIfClosed();

        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asTime(  );

        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        throwIfClosed();
        //TODO TH: how to get local calendar?
        //return accessValue( columnIndex ).asTimestamp(  );

        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asAsciiStream();
    }


    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asUnicodeStream();
    }


    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBinaryStream();
    }


    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(metadata.getColumnIndexFromLabel(columnLabel), scale);
    }


    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public SQLWarning getWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void clearWarnings() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public String getCursorName() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metadata;
    }


    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asObject();
    }


    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throwIfClosed();
        return metadata.getColumnIndexFromLabel(columnLabel);
    }


    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asCharacterStream();
    }


    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBigDecimal();
    }


    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public boolean isBeforeFirst() throws SQLException {
        return resultScroller.isBeforeFirst();
    }


    @Override
    public boolean isAfterLast() throws SQLException {
        return resultScroller.isAfterLast();
    }


    @Override
    public boolean isFirst() throws SQLException {
        return resultScroller.isFirst();
    }


    @Override
    public boolean isLast() throws SQLException {
        return resultScroller.isLast();
    }


    @Override
    public void beforeFirst() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        getBidirectionalScrollerOrThrow().beforeFirst();
    }


    @Override
    public void afterLast() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        getBidirectionalScrollerOrThrow().afterLast();
    }


    @Override
    public boolean first() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        return getBidirectionalScrollerOrThrow().first();
    }


    @Override
    public boolean last() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        try {
            return getBidirectionalScrollerOrThrow().last();
        } catch (InterruptedException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public int getRow() throws SQLException {
        return resultScroller.getRow();
    }


    @Override
    public boolean absolute(int i) throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        return getBidirectionalScrollerOrThrow().absolute(i);
    }


    @Override
    public boolean relative(int i) throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        return getBidirectionalScrollerOrThrow().relative(i);
    }


    @Override
    public boolean previous() throws SQLException {
        throwIfClosed();
        discardRowUpdates();
        return getBidirectionalScrollerOrThrow().previous();
    }


    @Override
    public void setFetchDirection(int fetchDirection) throws SQLException {
        throwIfClosed();
        if (properties.getResultSetType() == ResultSet.TYPE_FORWARD_ONLY && fetchDirection != ResultSet.FETCH_FORWARD) {
            throw new SQLException("Illegal fetch direction for resultset of TYPE_FORWARD_ONLY.");
        }
        properties.setFetchDirection(fetchDirection);
    }


    @Override
    public int getFetchDirection() throws SQLException {
        throwIfClosed();
        return properties.getFetchDirection();
    }


    @Override
    public void setFetchSize(int fetchSize) throws SQLException {
        throwIfClosed();
        if (fetchSize < 0) {
            throw new SQLException("Illegal value for fetch size. fetchSize >= 0 must hold.");
        }
        properties.setFetchSize(fetchSize);
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
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public boolean rowInserted() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public boolean rowDeleted() throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNull());
    }


    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBoolean(x));
    }


    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromByte(x));
    }


    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromShort(x));
    }


    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromInt(x));
    }


    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromLong(x));
    }


    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromFloat(x));
    }


    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromDouble(x));
    }


    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBigDecimal(x));
    }


    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromString(x));
    }


    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBytes(x));
    }


    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromDate(x));
    }


    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromTime(x));
    }


    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromTimestamp(x));
    }


    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        throwIfColumnIndexOutOfBounds(columnIndex);
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromAsciiStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBinaryStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int legnth) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromCharacterStream(x, legnth));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateObject(int columnIndex, Object x, int saleOrLength) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // TODO: propert implmentation. scaleOrLength only applies to streams (length) and bigdecimals(scale)
    }


    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromObject(x));
    }


    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateAsciiStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        updateBinaryStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateCharacterStream(String columnLabel, Reader x, int length) throws SQLException {
        updateCharacterStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrlength) throws SQLException {
        updateObject(metadata.getColumnIndexFromLabel(columnLabel), x, scaleOrlength);
    }


    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void insertRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void updateRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void deleteRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void refreshRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void cancelRowUpdates() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void moveToInsertRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public void moveToCurrentRow() throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public Statement getStatement() throws SQLException {
        throwIfClosed();
        return statement;
    }


    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        // saves time as exceptions don't have to be typed out by hand
        String methodName = new Object() {
        }.getClass().getEnclosingMethod().getName();
        throw new SQLException("Feature " + methodName + " not implemented");
    }


    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asRef();
    }


    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asBlob();
    }


    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asClob();
    }


    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asArray();
    }


    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(metadata.getColumnIndexFromLabel(columnLabel), map);
    }


    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Date getDate(int columnIndex, Calendar calendar) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asDate(calendar);
    }


    @Override
    public Date getDate(String columnLabel, Calendar calendar) throws SQLException {
        return getDate(metadata.getColumnIndexFromLabel(columnLabel), calendar);
    }


    @Override
    public Time getTime(int columnIndex, Calendar calendar) throws SQLException {
        return accessValue(columnIndex).asTime(calendar);
    }


    @Override
    public Time getTime(String columnLabel, Calendar calendar) throws SQLException {
        return getTime(metadata.getColumnIndexFromLabel(columnLabel), calendar);
    }


    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar calendar) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asTimestamp(calendar);
    }


    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar calendar) throws SQLException {
        return getTimestamp(metadata.getColumnIndexFromLabel(columnLabel), calendar);
    }


    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asUrl();
    }


    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromRef(x));
    }


    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBlob(x));
    }


    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromClob(x));
    }


    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromArray(x));
    }


    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asRowId();
    }


    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromRowId(x));
    }


    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(metadata.getColumnIndexFromLabel(columnLabel), x);
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
    public void updateNString(int columnIndex, String x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNString(x));
    }


    @Override
    public void updateNString(String columnLabel, String x) throws SQLException {
        updateNString(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateNClob(int columnIndex, NClob x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNClob(x));
    }


    @Override
    public void updateNClob(String columnLabel, NClob x) throws SQLException {
        updateNClob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asNClob();
    }


    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asSQLXML();
    }


    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public void updateSQLXML(int columnIndex, SQLXML x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromSQLXML(x));
    }


    @Override
    public void updateSQLXML(String columnLabel, SQLXML x) throws SQLException {
        updateSQLXML(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public String getNString(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asNString();
    }


    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asNCharacterStream();
    }


    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(metadata.getColumnIndexFromLabel(columnLabel));
    }


    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNCharacterStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateNCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        updateNCharacterStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromAsciiStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBinaryStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromCharacterStream(x, length));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateAsciiStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        updateBinaryStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateCharacterStream(String columnLabel, Reader x, long length) throws SQLException {
        updateCharacterStream(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateBlob(int columnIndex, InputStream x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBlob(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateBlob(String columnLabel, InputStream x, long length) throws SQLException {
        updateBlob(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateClob(int columnIndex, Reader x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromClob(x, length));
    }


    @Override
    public void updateClob(String columnLabel, Reader x, long length) throws SQLException {
        updateClob(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateNClob(int columnIndex, Reader x, long length) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNClob(x, length));
    }


    @Override
    public void updateNClob(String columnLabel, Reader x, long length) throws SQLException {
        updateNClob(metadata.getColumnIndexFromLabel(columnLabel), x, length);
    }


    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNCharacterStream(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateNCharacterStream(String columnLabel, Reader x) throws SQLException {
        updateNCharacterStream(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromAsciiStream(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBinaryStream(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromCharacterStream(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateCharacterStream(String columnLabel, Reader x) throws SQLException {
        updateCharacterStream(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateBlob(int columnIndex, InputStream x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        try {
            getOrCreateRowUpdate().put(columnIndex, TypedValue.fromBlob(x));
        } catch (IOException e) {
            throw new SQLException(e);
        }
    }


    @Override
    public void updateBlob(String columnLabel, InputStream x) throws SQLException {
        updateBlob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateClob(int columnIndex, Reader x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromClob(x));
    }


    @Override
    public void updateClob(String columnLabel, Reader x) throws SQLException {
        updateClob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public void updateNClob(int columnIndex, Reader x) throws SQLException {
        throwIfClosed();
        throwIfReadOnly();
        getOrCreateRowUpdate().put(columnIndex, TypedValue.fromNClob(x));
    }


    @Override
    public void updateNClob(String columnLabel, Reader x) throws SQLException {
        updateNClob(metadata.getColumnIndexFromLabel(columnLabel), x);
    }


    @Override
    public <T> T getObject(int columnIndex, Class<T> aClass) throws SQLException {
        throwIfClosed();
        return accessValue(columnIndex).asObject(aClass);
    }


    @Override
    public <T> T getObject(String columnLabel, Class<T> aClass) throws SQLException {
        return getObject(metadata.getColumnIndexFromLabel(columnLabel), aClass);
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

/*
 * Copyright 2019-2024 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.polypheny.jdbc.meta.PolyphenyParameterMetaData;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.jdbc.streaming.StreamingIndex;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.Frame;
import org.polypheny.prism.PreparedStatementSignature;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResult;

public class PolyphenyPreparedStatement extends PolyphenyStatement implements PreparedStatement {

    private TypedValue[] parameters;
    private List<List<TypedValue>> parameterBatch = new LinkedList<>();
    private final PolyphenyParameterMetaData parameterMetaData;
    private final StreamingIndex streamingIndex = new StreamingIndex(getClient());


    public PolyphenyPreparedStatement( PolyConnection connection, PolyphenyStatementProperties properties, PreparedStatementSignature statementSignature ) throws SQLException {
        super( connection, properties );
        this.statementId = statementSignature.getStatementId();
        streamingIndex.update( statementSignature.getStatementId() );
        this.parameterMetaData = new PolyphenyParameterMetaData( statementSignature );
        this.parameters = createParameterList( statementSignature.getParameterMetasCount() );
    }


    private void prepareForReExecution() throws SQLException {
        if ( currentResult != null ) {
            currentResult.close();
        }
        currentUpdateCount = NO_UPDATE_COUNT;
    }


    private TypedValue[] createParameterList( int parameterCount ) {
        return new TypedValue[parameterCount];
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public boolean execute( String statement ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public boolean execute( String s, int i ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );

    }


    @Override
    public boolean execute( String s, int[] ints ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );

    }


    @Override
    public boolean execute( String s, String[] strings ) throws SQLException {
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        try {
            throwIfClosed();
            prepareForReExecution();
            StatementResult result = getClient().executeIndexedStatement(
                    statementId,
                    Arrays.asList( parameters ),
                    properties.getFetchSize(),
                    streamingIndex,
                    getTimeout()
            );
            if ( !result.hasFrame() ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a single ResultSet" );
            }
            Frame frame = result.getFrame();
            throwIfNotRelational( frame );
            currentResult = new PolyphenyResultSet( this, frame, properties.toResultSetProperties() );
            return currentResult;
        } finally {
            clearParameters();
            clearParameterBatch();
        }
    }


    @Override
    public long executeLargeUpdate() throws SQLException {
        try {
            throwIfClosed();
            prepareForReExecution();
            StatementResult result = getClient().executeIndexedStatement(
                    statementId,
                    Arrays.asList( parameters ),
                    properties.getFetchSize(),
                    streamingIndex,
                    getTimeout()
            );
            if ( result.hasFrame() ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Statement must not produce a ResultSet" );
            }
            currentUpdateCount = result.getScalar();
            return currentUpdateCount;
        } finally {
            clearParameters();
            clearParameterBatch();
        }
    }


    @Override
    public int executeUpdate() throws SQLException {
        return longToInt( executeLargeUpdate() );
    }


    private void throwIfOutOfBounds( int parameterIndex ) throws SQLException {
        if ( parameterIndex < 1 ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds." );
        }
        if ( parameterIndex > parameterMetaData.getParameterCount() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds." );
        }
    }


    private int indexFromParameterIndex( int parameterIndex ) {
        return parameterIndex - 1;
    }


    @Override
    public void setNull( int parameterIndex, int sqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNull();
    }


    @Override
    public void setBoolean( int parameterIndex, boolean x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBoolean( x );
    }


    @Override
    public void setByte( int parameterIndex, byte x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromByte( x );
    }


    @Override
    public void setShort( int parameterIndex, short x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromShort( x );
    }


    @Override
    public void setInt( int parameterIndex, int x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromInteger( x );
    }


    @Override
    public void setLong( int parameterIndex, long x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromLong( x );
    }


    @Override
    public void setFloat( int parameterIndex, float x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromFloat( x );
    }


    @Override
    public void setDouble( int parameterIndex, double x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromDouble( x );
    }


    @Override
    public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBigDecimal( x );
    }


    @Override
    public void setString( int parameterIndex, String x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromString( x );
    }


    @Override
    public void setBytes( int parameterIndex, byte[] x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBytes( x );
    }


    @Override
    public void setDate( int parameterIndex, Date x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromDate( x );
    }


    @Override
    public void setTime( int parameterIndex, Time x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromTime( x );
    }


    @Override
    public void setTimestamp( int parameterIndex, Timestamp x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromTimestamp( x );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x, length );
    }


    @Override
    public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromUnicodeStream( x, length );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x, length );
    }


    @Override
    public void clearParameters() throws SQLException {
        throwIfClosed();
        parameters = createParameterList( parameterMetaData.getParameterCount() );
    }


    private void clearParameterBatch() {
        parameterBatch = new LinkedList<>();
    }


    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromObject( x, targetSqlType );
    }


    @Override
    public void setObject( int parameterIndex, Object x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromObject( x );
    }


    @Override
    public boolean execute() throws SQLException {
        try {
            throwIfClosed();
            prepareForReExecution();
            StatementResult result = getClient().executeIndexedStatement(
                    statementId,
                    Arrays.asList( parameters ),
                    properties.getFetchSize(),
                    streamingIndex,
                    getTimeout()
            );
            if ( !result.hasFrame() ) {
                currentUpdateCount = result.getScalar();
                return false;
            }
            Frame frame = result.getFrame();
            throwIfNotRelational( frame );
            currentResult = new PolyphenyResultSet( this, frame, properties.toResultSetProperties() );
            return true;
        } finally {
            clearParameters();
            clearParameterBatch();
        }
    }


    @Override
    public void addBatch() throws SQLException {
        throwIfClosed();
        parameterBatch.add( Arrays.asList( parameters.clone() ) );
    }


    @Override
    public long[] executeLargeBatch() throws SQLException {
        List<Long> scalars = executeParameterizedBatch();
        long[] updateCounts = new long[scalars.size()];
        for ( int i = 0; i < scalars.size(); i++ ) {
            updateCounts[i] = scalars.get( i );
        }
        return updateCounts;
    }


    @Override
    public int[] executeBatch() throws SQLException {
        List<Long> scalars = executeParameterizedBatch();
        int[] updateCounts = new int[scalars.size()];
        for ( int i = 0; i < scalars.size(); i++ ) {
            updateCounts[i] = longToInt( scalars.get( i ) );
        }
        return updateCounts;
    }


    private List<Long> executeParameterizedBatch() throws SQLException {
        throwIfClosed();
        try {
            StatementBatchResponse status = getClient().executeIndexedStatementBatch( statementId, parameterBatch, streamingIndex, getTimeout() );
            return status.getScalarsList();
        } finally {
            // jdbc: batch and individual parameters are always cleared even in the execution fails.
            clearParameters();
            clearParameterBatch();
        }
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setRef( int parameterIndex, Ref x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromRef( x );
    }


    @Override
    public void setBlob( int parameterIndex, Blob x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBlob( x );
    }


    @Override
    public void setClob( int parameterIndex, Clob x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromClob( x );
    }


    @Override
    public void setArray( int parameterIndex, Array x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromArray( x );
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }


    @Override
    public void setDate( int parameterIndex, Date x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromDate( x, cal );
    }


    @Override
    public void setTime( int parameterIndex, Time x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromTime( x, cal );
    }


    @Override
    public void setTimestamp( int parameterIndex, Timestamp x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromTimestamp( x, cal );
    }


    @Override
    public void setNull( int parameterIndex, int sqlType, String typeName ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNull();
    }


    @Override
    public void setURL( int parameterIndex, URL x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromUrl( x );
    }


    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return parameterMetaData;
    }


    @Override
    public void setRowId( int parameterIndex, RowId x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromRowId( x );
    }


    @Override
    public void setNString( int parameterIndex, String value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNString( value );
    }


    @Override
    public void setNCharacterStream( int parameterIndex, Reader value, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNCharacterStream( value );
    }


    @Override
    public void setNClob( int parameterIndex, NClob value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNClob( value );
    }


    @Override
    public void setClob( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromClob( reader, length );
    }


    @Override
    public void setBlob( int parameterIndex, InputStream inputStream, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBlob( inputStream, length );
    }


    @Override
    public void setNClob( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNClob( reader, length );
    }


    @Override
    public void setSQLXML( int parameterIndex, SQLXML xmlObject ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromSQLXML( xmlObject );
    }


    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType, int scaleOrLength ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromObject( x, targetSqlType, scaleOrLength );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x, length );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x, length );
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromCharacterStream( reader, length );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x );
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromCharacterStream( reader );
    }


    @Override
    public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNCharacterStream( value );
    }


    @Override
    public void setClob( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromClob( reader );
    }


    @Override
    public void setBlob( int parameterIndex, InputStream inputStream ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBlob( inputStream );
    }


    @Override
    public void setNClob( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNClob( reader );
    }

}

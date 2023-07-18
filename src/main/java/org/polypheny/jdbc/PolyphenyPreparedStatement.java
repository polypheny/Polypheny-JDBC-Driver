package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
import java.io.IOException;
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
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.proto.StatementBatchStatus;
import org.polypheny.jdbc.proto.StatementResult;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.jdbc.utils.CallbackQueue;

public class PolyphenyPreparedStatement extends PolyphenyStatement implements PreparedStatement {

    private List<TypedValue> parameters;
    private List<List<TypedValue>> parameterBatch;
    private PolyphenyParameterMetaData parameterMetaData;


    public PolyphenyPreparedStatement(PolyphenyConnection connection, PolyphenyStatementProperties properties, PreparedStatementSignature statementSignature ) throws SQLException {
        super( connection, properties );
        this.statementId = statementSignature.getStatementId();
        this.parameterMetaData = new PolyphenyParameterMetaData( statementSignature );
        this.parameters = createParamterList( statementSignature.getParameterMetasCount() );
        this.parameterBatch = new LinkedList<>();
    }


    private List<TypedValue> createParamterList( int parameterCount ) {
        return Arrays.asList( new TypedValue[parameterCount] );
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        throwIfClosed();
        int timeout = properties.getQueryTimeoutSeconds();
        try {
            StatementResult result = getClient().executeIndexedStatement( timeout, statementId, parameters );
            closeCurrentResult();
            if ( !result.hasFrame() ) {
                throw new SQLException( "Statement must produce a single ResultSet" );
            }
            Frame frame = result.getFrame();
            throwIfNotRelational( frame );
            currentResult = createResultSet( frame );
            return currentResult;
        } catch ( StatusRuntimeException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public long executeLargeUpdate() throws SQLException {
        throwIfClosed();
        int timeout = properties.getQueryTimeoutSeconds();
        try {
            StatementResult result = getClient().executeIndexedStatement( timeout, statementId, parameters );
            closeCurrentResult();
            if ( result.hasFrame() ) {
                throw new SQLException( "Statement must not produce a ResultSet" );
            }
            currentUpdateCount = result.getScalar();
            return currentUpdateCount;
        } catch ( StatusRuntimeException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public int executeUpdate() throws SQLException {
        return longToInt( executeLargeUpdate() );
    }


    private void throwIfOutOfBounds( int parameterIndex ) throws SQLException {
        if ( parameterIndex < 1 ) {
            throw new SQLException( "index out of bounds." );
        }
        if ( parameterIndex > parameterMetaData.getParameterCount() ) {
            throw new SQLException( "index out of bounds." );
        }
    }


    private int indexFromParameterIndex( int parameterIndex ) {
        return parameterIndex - 1;
    }


    @Override
    public void setNull( int parameterIndex, int sqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromNull( sqlType ) );
    }


    @Override
    public void setBoolean( int parameterIndex, boolean x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromBoolean( x ) );
    }


    @Override
    public void setByte( int parameterIndex, byte x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromByte( x ) );
    }


    @Override
    public void setShort( int parameterIndex, short x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromShort( x ) );
    }


    @Override
    public void setInt( int parameterIndex, int x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromInt( x ) );
    }


    @Override
    public void setLong( int parameterIndex, long x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromLong( x ) );
    }


    @Override
    public void setFloat( int parameterIndex, float x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromFloat( x ) );
    }


    @Override
    public void setDouble( int parameterIndex, double x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromDouble( x ) );
    }


    @Override
    public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromBigDecimal( x ) );
    }


    @Override
    public void setString( int parameterIndex, String x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromString( x ) );
    }


    @Override
    public void setBytes( int parameterIndex, byte[] x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromBytes( x ) );
    }


    @Override
    public void setDate( int parameterIndex, Date x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromDate( x ) );
    }


    @Override
    public void setTime( int parameterIndex, Time x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromTime( x ) );
    }


    @Override
    public void setTimestamp( int parameterIndex, Timestamp x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromTimestamp( x ) );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromAsciiStream( x, length ) );
        } catch ( IOException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromUnicodeStream( x, length ) );
        } catch ( IOException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromBinaryStream( x, length ) );
        } catch ( IOException e ) {
            throw new SQLException( e );
        }
    }


    @Override
    public void clearParameters() throws SQLException {
        throwIfClosed();
        parameters = createParamterList( parameterMetaData.getParameterCount() );
    }


    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromObject( x, targetSqlType ) );
    }


    @Override
    public void setObject( int parameterIndex, Object x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters.set( indexFromParameterIndex( parameterIndex ), TypedValue.fromObject( x ) );
    }


    @Override
    public boolean execute() throws SQLException {
        throwIfClosed();
        int timeout = properties.getQueryTimeoutSeconds();
        try {
            StatementResult result = getClient().executeIndexedStatement( timeout, statementId, parameters );
            closeCurrentResult();
            if ( !result.hasFrame() ) {
                currentUpdateCount = result.getScalar();
                return false;
            }
            Frame frame = result.getFrame();
            throwIfNotRelational( frame );
            currentResult = createResultSet( frame );
            return true;
        } catch ( StatusRuntimeException e ) {
            throw new SQLException( e.getMessage() );
        }
    }


    @Override
    public void addBatch() throws SQLException {
        throwIfClosed();
        parameterBatch.add( parameters );
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
        discardStatementId();
        int timeout = properties.getQueryTimeoutSeconds();
        CallbackQueue<StatementBatchStatus> callback = new CallbackQueue<>();
        try {
            StatementBatchStatus status = getClient().executeIndexedStatementBatch( timeout, statementId, parameterBatch );
            return status.getScalarsList();
        } catch ( ProtoInterfaceServiceException e ) {
            throw new SQLException( e.getMessage() );
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
    }


    @Override
    public void setBlob( int parameterIndex, Blob x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setClob( int parameterIndex, Clob x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setArray( int parameterIndex, Array x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return null;
    }


    @Override
    public void setDate( int parameterIndex, Date x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setTime( int parameterIndex, Time x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setTimestamp( int parameterIndex, Timestamp x, Calendar cal ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNull( int parameterIndex, int sqlType, String typeName ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setURL( int parameterIndex, URL x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return parameterMetaData;
    }


    @Override
    public void setRowId( int parameterIndex, RowId x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNString( int parameterIndex, String value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNCharacterStream( int parameterIndex, Reader value, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNClob( int parameterIndex, NClob value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setClob( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBlob( int parameterIndex, InputStream inputStream, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNClob( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setSQLXML( int parameterIndex, SQLXML xmlObject ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setObject( int parameterIndex, Object x, int targetSqlType, int scaleOrLength ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setClob( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBlob( int parameterIndex, InputStream inputStream ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setNClob( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }

}

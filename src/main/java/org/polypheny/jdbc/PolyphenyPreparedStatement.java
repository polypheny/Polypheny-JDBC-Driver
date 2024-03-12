package org.polypheny.jdbc;

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import org.polypheny.jdbc.meta.PolyphenyParameterMetaData;
import org.polypheny.jdbc.properties.PolyphenyStatementProperties;
import org.polypheny.db.protointerface.proto.Frame;
import org.polypheny.db.protointerface.proto.PreparedStatementSignature;
import org.polypheny.db.protointerface.proto.StatementBatchResponse;
import org.polypheny.db.protointerface.proto.StatementResult;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class PolyphenyPreparedStatement extends PolyphenyStatement implements PreparedStatement {

    private TypedValue[] parameters;
    private List<List<TypedValue>> parameterBatch;
    private PolyphenyParameterMetaData parameterMetaData;


    public PolyphenyPreparedStatement( PolyConnection connection, PolyphenyStatementProperties properties, PreparedStatementSignature statementSignature ) throws SQLException {
        super( connection, properties );
        this.statementId = statementSignature.getStatementId();
        this.parameterMetaData = new PolyphenyParameterMetaData( statementSignature );
        this.parameters = createParameterList( statementSignature.getParameterMetasCount() );
        this.parameterBatch = new LinkedList<>();
    }


    private TypedValue[] createParameterList( int parameterCount ) {
        return new TypedValue[parameterCount];
    }


    @Override
    public ResultSet executeQuery( String statement ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String statement ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public boolean execute( String statement ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, int autogeneratedKeys ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, int[] columnIndexes ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public long executeLargeUpdate( String sql, String[] columnNames ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public int executeUpdate( String sql, String[] columnNames ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public boolean execute( String s, int i ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );

    }


    @Override
    public boolean execute( String s, int[] ints ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );

    }


    @Override
    public boolean execute( String s, String[] strings ) throws SQLException {
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Method should not be called on a prepared statement." );
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        throwIfClosed();

        StatementResult result = getClient().executeIndexedStatement(
                statementId,
                Arrays.asList( parameters ),
                properties.getFetchSize(),
                getTimeout()
        );
        closeCurrentResult();
        if ( !result.hasFrame() ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.RESULT_TYPE_INVALID, "Statement must produce a single ResultSet" );
        }
        Frame frame = result.getFrame();
        throwIfNotRelational( frame );
        currentResult = new PolyhenyResultSet( this, frame, properties.toResultSetProperties() );
        return currentResult;
    }


    @Override
    public long executeLargeUpdate() throws SQLException {
        throwIfClosed();
        StatementResult result = getClient().executeIndexedStatement(
                statementId,
                Arrays.asList( parameters ),
                properties.getFetchSize(),
                getTimeout()
        );
        closeCurrentResult();
        if ( result.hasFrame() ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.RESULT_TYPE_INVALID, "Statement must not produce a ResultSet" );
        }
        currentUpdateCount = result.getScalar();
        return currentUpdateCount;
    }


    @Override
    public int executeUpdate() throws SQLException {
        return longToInt( executeLargeUpdate() );
    }


    private void throwIfOutOfBounds( int parameterIndex ) throws SQLException {
        if ( parameterIndex < 1 ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds." );
        }
        if ( parameterIndex > parameterMetaData.getParameterCount() ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.VALUE_ILLEGAL, "Index out of bounds." );
        }
    }


    private int indexFromParameterIndex( int parameterIndex ) {
        return parameterIndex - 1;
    }


    @Override
    public void setNull( int parameterIndex, int sqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNull( sqlType );
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
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Handling stream failed.", e );
        }
    }


    @Override
    public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromUnicodeStream( x, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Handling stream failed.", e );
        }
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Handling stream failed.", e );
        }
    }


    @Override
    public void clearParameters() throws SQLException {
        throwIfClosed();
        parameters = createParameterList( parameterMetaData.getParameterCount() );
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
        throwIfClosed();
        StatementResult result = getClient().executeIndexedStatement(
                statementId,
                Arrays.asList( parameters ),
                properties.getFetchSize(),
                getTimeout()
        );
        closeCurrentResult();
        if ( !result.hasFrame() ) {
            currentUpdateCount = result.getScalar();
            return false;
        }
        Frame frame = result.getFrame();
        throwIfNotRelational( frame );
        currentResult = new PolyhenyResultSet( this, frame, properties.toResultSetProperties() );
        return true;
    }


    @Override
    public void addBatch() throws SQLException {
        throwIfClosed();
        parameterBatch.add( new ArrayList<>( Arrays.asList( parameters ) ) );
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
        StatementBatchResponse status = getClient().executeIndexedStatementBatch( statementId, parameterBatch, getTimeout() );
        clearParameters();
        return status.getScalarsList();
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
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNull( sqlType, typeName );
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
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNCharacterStream( value );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting NCharacterStream.", e );
        }
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
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting AsciiStream.", e );
        }
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting BinaryStream.", e );
        }
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromCharacterStream( reader, length );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting CharacterStream.", e );
        }
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromAsciiStream( x );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting AsciiStream.", e );
        }
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBinaryStream( x );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting BinaryStream.", e );
        }
    }


    @Override
    public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromCharacterStream( reader );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting CharacterStream.", e );
        }
    }


    @Override
    public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNCharacterStream( value );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting NCharacterStream.", e );
        }
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
        try {
            parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromBlob( inputStream );
        } catch ( IOException e ) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.STREAM_ERROR, "Error while setting Blob.", e );
        }
    }


    @Override
    public void setNClob( int parameterIndex, Reader reader ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
        parameters[indexFromParameterIndex( parameterIndex )] = TypedValue.fromNClob( reader );
    }

}

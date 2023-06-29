package org.polypheny.jdbc;

import io.grpc.StatusRuntimeException;
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
import java.util.List;
import org.polypheny.jdbc.proto.Frame;
import org.polypheny.jdbc.proto.PreparedStatementSignature;
import org.polypheny.jdbc.proto.StatementResult;
import org.polypheny.jdbc.types.TypedValue;

public class PolyphenyPreparedStatement extends PolyphenyStatement implements PreparedStatement {

    private List<TypedValue> parameters;
    private PolyphenyParameterMetaData parameterMetaData;


    public PolyphenyPreparedStatement( PolyphenyConnection connection, StatementProperties properties, PreparedStatementSignature statementSignature ) {
        super( connection, properties );
        this.statementId = statementSignature.getStatementId();
        this.parameterMetaData = new PolyphenyParameterMetaData( statementSignature );
        this.parameters = createParamterList( statementSignature.getParameterMetasCount() );
    }


    private List<TypedValue> createParamterList( int parameterCount ) {
        return Arrays.asList( new TypedValue[parameterCount] );
    }


    @Override
    public ResultSet executeQuery() throws SQLException {
        throwIfClosed();
        try {
            StatementResult result = getClient().executePreparedStatement( statementId, parameters );
            resetCurrentResults();
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
        try {
            StatementResult result = getClient().executePreparedStatement( statementId, parameters );
            resetCurrentResults();
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


    @Override
    public void setNull( int parameterIndex, int sqlType ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBoolean( int parameterIndex, boolean x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setByte( int parameterIndex, byte x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setShort( int parameterIndex, short x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setInt( int parameterIndex, int x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setLong( int parameterIndex, long x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setFloat( int parameterIndex, float x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setDouble( int parameterIndex, double x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBigDecimal( int parameterIndex, BigDecimal x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setString( int parameterIndex, String x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBytes( int parameterIndex, byte[] x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setDate( int parameterIndex, Date x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setTime( int parameterIndex, Time x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setTimestamp( int parameterIndex, Timestamp x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setAsciiStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setUnicodeStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public void setBinaryStream( int parameterIndex, InputStream x, int length ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
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
    }


    @Override
    public void setObject( int parameterIndex, Object x ) throws SQLException {
        throwIfClosed();
        throwIfOutOfBounds( parameterIndex );
    }


    @Override
    public boolean execute() throws SQLException {
        throwIfClosed();
        try {
            StatementResult result = getClient().executePreparedStatement( statementId, parameters );
            resetCurrentResults();
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

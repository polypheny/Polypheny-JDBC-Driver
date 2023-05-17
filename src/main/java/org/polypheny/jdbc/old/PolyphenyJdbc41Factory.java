/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.jdbc.old;


import java.io.InputStream;
import java.io.Reader;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaDatabaseMetaData;
import org.apache.calcite.avatica.AvaticaFactory;
import org.apache.calcite.avatica.AvaticaPreparedStatement;
import org.apache.calcite.avatica.AvaticaResultSet;
import org.apache.calcite.avatica.AvaticaResultSetMetaData;
import org.apache.calcite.avatica.AvaticaStatement;
import org.apache.calcite.avatica.Meta.Frame;
import org.apache.calcite.avatica.Meta.Signature;
import org.apache.calcite.avatica.Meta.StatementHandle;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.UnregisteredDriver;


/**
 * See also org.apache.calcite.avatica.AvaticaJdbc41Factory
 */
@SuppressWarnings("unused")
public class PolyphenyJdbc41Factory extends PolyphenyJdbcFactory {

    /**
     * Creates a factory for JDBC version 4.1.
     */
    public PolyphenyJdbc41Factory() {
        this( 4, 1 );
    }


    /**
     * Creates a JDBC factory with given major/minor version number.
     *
     * @param major JDBC major version
     * @param minor JDBC minor version
     */
    protected PolyphenyJdbc41Factory( int major, int minor ) {
        super( major, minor );
    }


    @Override
    public PolyphenyDbJdbc41Connection newConnection( final UnregisteredDriver driver, final AvaticaFactory factory, final String url, final Properties info ) {
        return new PolyphenyDbJdbc41Connection( driver, factory, url, info );
    }


    @Override
    public PolyphenyDbJdbc41Statement newStatement( AvaticaConnection connection, StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
        return new PolyphenyDbJdbc41Statement( connection, h, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    @Override
    public PolyphenyDbJdbc41PreparedStatement newPreparedStatement( AvaticaConnection connection, StatementHandle h, Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
        return new PolyphenyDbJdbc41PreparedStatement( connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability );
    }


    @Override
    public PolyphenyDbJdbc41ResultSet newResultSet( AvaticaStatement statement, QueryState state, Signature signature, TimeZone timeZone, Frame firstFrame ) throws SQLException {
        final PolyphenyDbJdbc41ResultSetMetaData metaData = newResultSetMetaData( statement, signature );
        return new PolyphenyDbJdbc41ResultSet( statement, state, signature, metaData, timeZone, firstFrame );
    }


    @Override
    public PolyphenyDbJdbc41DatabaseMetaData newDatabaseMetaData( AvaticaConnection connection ) {
        return new PolyphenyDbJdbc41DatabaseMetaData( connection );
    }


    @Override
    public PolyphenyDbJdbc41ResultSetMetaData newResultSetMetaData( AvaticaStatement statement, Signature signature ) {
        return new PolyphenyDbJdbc41ResultSetMetaData( statement, signature );
    }


    /**
     * See also org.apache.calcite.avatica.AvaticaJdbc41Factory.AvaticaJdbc41Connection
     */
    protected static class PolyphenyDbJdbc41Connection extends AvaticaConnection implements PolyphenyJdbcConnection {

        /**
         * @param driver Driver
         * @param factory Factory for JDBC objects
         * @param url Server URL
         * @param info Other connection properties
         */
        protected PolyphenyDbJdbc41Connection( final UnregisteredDriver driver, final AvaticaFactory factory, final String url, final Properties info ) {
            super( driver, factory, url, info );
        }
    }


    /**
     * See also org.apache.calcite.avatica.AvaticaJdbc41Factory.AvaticaJdbc41Statement
     */
    protected static class PolyphenyDbJdbc41Statement extends AvaticaStatement implements PolyphenyJdbcStatement {

        protected PolyphenyDbJdbc41Statement( AvaticaConnection connection, StatementHandle h, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) {
            super( connection, h, resultSetType, resultSetConcurrency, resultSetHoldability );
        }
    }


    /**
     * See also org.apache.calcite.avatica.AvaticaJdbc41Factory.AvaticaJdbc41PreparedStatement
     */
    protected static class PolyphenyDbJdbc41PreparedStatement extends AvaticaPreparedStatement implements PolyphenyJdbcPreparedStatement {

        /**
         * @param connection Connection
         * @param h Statement handle
         * @param signature Result of preparing statement
         * @param resultSetType Result set type
         * @param resultSetConcurrency Result set concurrency
         * @param resultSetHoldability Result set holdability
         * @throws SQLException If fails due to underlying implementation reasons.
         */
        protected PolyphenyDbJdbc41PreparedStatement( AvaticaConnection connection, StatementHandle h, Signature signature, int resultSetType, int resultSetConcurrency, int resultSetHoldability ) throws SQLException {
            super( connection, h, signature, resultSetType, resultSetConcurrency, resultSetHoldability );
        }


        @Override
        public void setRowId( int parameterIndex, RowId x ) throws SQLException {
            getSite( parameterIndex ).setRowId( x );
        }


        @Override
        public void setNString( int parameterIndex, String value ) throws SQLException {
            getSite( parameterIndex ).setNString( value );
        }


        @Override
        public void setNCharacterStream( int parameterIndex, Reader value, long length ) throws SQLException {
            getSite( parameterIndex ).setNCharacterStream( value, length );
        }


        @Override
        public void setNClob( int parameterIndex, NClob value ) throws SQLException {
            getSite( parameterIndex ).setNClob( value );
        }


        @Override
        public void setClob( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setClob( reader, length );
        }


        @Override
        public void setBlob( int parameterIndex, InputStream inputStream, long length ) throws SQLException {
            getSite( parameterIndex ).setBlob( inputStream, length );
        }


        @Override
        public void setNClob( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setNClob( reader, length );
        }


        @Override
        public void setSQLXML( int parameterIndex, SQLXML xmlObject ) throws SQLException {
            getSite( parameterIndex ).setSQLXML( xmlObject );
        }


        @Override
        public void setAsciiStream( int parameterIndex, InputStream x, long length ) throws SQLException {
            getSite( parameterIndex ).setAsciiStream( x, length );
        }


        @Override
        public void setBinaryStream( int parameterIndex, InputStream x, long length ) throws SQLException {
            getSite( parameterIndex ).setBinaryStream( x, length );
        }


        @Override
        public void setCharacterStream( int parameterIndex, Reader reader, long length ) throws SQLException {
            getSite( parameterIndex ).setCharacterStream( reader, length );
        }


        @Override
        public void setAsciiStream( int parameterIndex, InputStream x ) throws SQLException {
            getSite( parameterIndex ).setAsciiStream( x );
        }


        @Override
        public void setBinaryStream( int parameterIndex, InputStream x ) throws SQLException {
            getSite( parameterIndex ).setBinaryStream( x );
        }


        @Override
        public void setCharacterStream( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setCharacterStream( reader );
        }


        @Override
        public void setNCharacterStream( int parameterIndex, Reader value ) throws SQLException {
            getSite( parameterIndex ).setNCharacterStream( value );
        }


        @Override
        public void setClob( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setClob( reader );
        }


        @Override
        public void setBlob( int parameterIndex, InputStream inputStream ) throws SQLException {
            getSite( parameterIndex ).setBlob( inputStream );
        }


        @Override
        public void setNClob( int parameterIndex, Reader reader ) throws SQLException {
            getSite( parameterIndex ).setNClob( reader );
        }
    }


    protected static class PolyphenyDbJdbc41ResultSet extends AvaticaResultSet implements PolyphenyJdbcResultSet {

        protected PolyphenyDbJdbc41ResultSet( AvaticaStatement statement, QueryState state, Signature signature, PolyphenyDbJdbc41ResultSetMetaData resultSetMetaData, TimeZone timeZone, Frame firstFrame ) throws SQLException {
            super( statement, state, signature, resultSetMetaData, timeZone, firstFrame );
        }
    }


    protected static class PolyphenyDbJdbc41DatabaseMetaData extends AvaticaDatabaseMetaData implements PolyphenyJdbcDatabaseMetaData {

        protected PolyphenyDbJdbc41DatabaseMetaData( AvaticaConnection connection ) {
            super( connection );
        }
    }


    protected static class PolyphenyDbJdbc41ResultSetMetaData extends AvaticaResultSetMetaData implements PolyphenyJdbcResultSetMetaData {

        protected PolyphenyDbJdbc41ResultSetMetaData( AvaticaStatement statement, Signature signature ) {
            /*
             * See also org.apache.calcite.avatica.AvaticaJdbc41Factory#newResultSetMetaData(AvaticaStatement, Signature)
             */
            super( statement, null, signature );
        }
    }
}

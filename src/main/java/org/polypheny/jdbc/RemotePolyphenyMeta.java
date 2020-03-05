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

package org.polypheny.jdbc;


import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.AvaticaUtils;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ConnectionPropertiesImpl;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.calcite.avatica.MissingResultsException;
import org.apache.calcite.avatica.NoSuchStatementException;
import org.apache.calcite.avatica.QueryState;
import org.apache.calcite.avatica.remote.Service;
import org.apache.calcite.avatica.remote.Service.CloseConnectionRequest;
import org.apache.calcite.avatica.remote.Service.CloseConnectionResponse;
import org.apache.calcite.avatica.remote.Service.CloseStatementRequest;
import org.apache.calcite.avatica.remote.Service.CommitRequest;
import org.apache.calcite.avatica.remote.Service.OpenConnectionRequest;
import org.apache.calcite.avatica.remote.Service.RollbackRequest;
import org.apache.calcite.avatica.remote.TypedValue;


/**
 * see org.apache.calcite.avatica.remote.RemoteMeta
 */
class RemotePolyphenyMeta extends MetaImpl {

    final Service service;
    final Map<String, ConnectionPropertiesImpl> propsMap = new HashMap<>();
    private Map<DatabaseProperty, Object> databaseProperties;


    RemotePolyphenyMeta( final AvaticaConnection connection ) {
        super( connection );
        this.service = connection.getService();
    }


    RemotePolyphenyMeta( final AvaticaConnection connection, final Service service ) {
        super( connection );
        this.service = service;
    }


    private MetaResultSet toResultSet( final Class clazz, final Service.ResultSetResponse response ) {
        if ( response.updateCount != -1 ) {
            return MetaResultSet.count( response.connectionId, response.statementId, response.updateCount );
        }
        Signature signature0 = response.signature;
        if ( signature0 == null ) {
            final List<ColumnMetaData> columns =
                    clazz == null
                            ? Collections.emptyList()
                            : fieldMetaData( clazz ).columns;

            signature0 = Signature.create( columns, "?", Collections.emptyList(), response.signature.cursorFactory, Meta.StatementType.SELECT );
        }
        return MetaResultSet.create( response.connectionId, response.statementId, response.ownStatement, signature0, response.firstFrame );
    }


    @Override
    public Map<DatabaseProperty, Object> getDatabaseProperties( final ConnectionHandle connectionHandle ) {
        synchronized ( this ) {
            // Compute map on first use, and cache
            if ( databaseProperties == null ) {
                databaseProperties = service.apply( new Service.DatabasePropertyRequest( connectionHandle.id ) ).map;
            }
            return databaseProperties;
        }
    }


    @Override
    public StatementHandle createStatement( final ConnectionHandle connectionHandle ) {
        return connection.invokeWithRetries( () -> {
            // sync connection state if necessary
            connectionSync( connectionHandle, new ConnectionPropertiesImpl() );
            final Service.CreateStatementResponse response = service.apply( new Service.CreateStatementRequest( connectionHandle.id ) );
            return new StatementHandle( response.connectionId, response.statementId, null );
        } );
    }


    @Override
    public void closeStatement( final StatementHandle statementHandle ) {
        connection.invokeWithRetries( () -> service.apply( new CloseStatementRequest( statementHandle.connectionId, statementHandle.id ) ) );
    }


    @Override
    public void openConnection( final ConnectionHandle connectionHandle, final Map<String, String> info ) {
        connection.invokeWithRetries( () -> service.apply( new OpenConnectionRequest( connectionHandle.id, info ) ) );
    }


    @Override
    public void closeConnection( final ConnectionHandle connectionHandle ) {
        connection.invokeWithRetries( () -> {
            final CloseConnectionResponse response = service.apply( new CloseConnectionRequest( connectionHandle.id ) );
            propsMap.remove( connectionHandle.id );
            return response;
        } );
    }


    @Override
    public ConnectionProperties connectionSync( final ConnectionHandle connectionHandle, final ConnectionProperties connProps ) {
        return connection.invokeWithRetries(
                () -> {
                    ConnectionPropertiesImpl localProps = propsMap.get( connectionHandle.id );
                    if ( localProps == null ) {
                        localProps = new ConnectionPropertiesImpl();
                        localProps.setDirty( true );
                        propsMap.put( connectionHandle.id, localProps );
                    }

                    // Only make an RPC if necessary. RPC is necessary when we have local changes that need flushed to the server (be sure to introduce any new changes from connProps before
                    // checking AND when connProps.isEmpty() (meaning, this was a request for a value, not overriding a value). Otherwise, accumulate the change locally and return immediately.
                    if ( localProps.merge( connProps ).isDirty() && connProps.isEmpty() ) {
                        final Service.ConnectionSyncResponse response = service.apply( new Service.ConnectionSyncRequest( connectionHandle.id, localProps ) );
                        propsMap.put( connectionHandle.id, (ConnectionPropertiesImpl) response.connProps );
                        return response.connProps;
                    } else {
                        return localProps;
                    }
                } );
    }


    @Override
    public MetaResultSet getCatalogs( final ConnectionHandle connectionHandle ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.CatalogsRequest( connectionHandle.id ) );
            return toResultSet( MetaCatalog.class, response );
        } );
    }


    @Override
    public MetaResultSet getSchemas( final ConnectionHandle connectionHandle, final String catalog, final Pat schemaPattern ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.SchemasRequest( connectionHandle.id, catalog, schemaPattern.s ) );
            return toResultSet( MetaSchema.class, response );
        } );
    }


    @Override
    public MetaResultSet getTables( final ConnectionHandle connectionHandle, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final List<String> typeList ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.TablesRequest( connectionHandle.id, catalog, schemaPattern.s, tableNamePattern.s, typeList ) );
            return toResultSet( MetaTable.class, response );
        } );
    }


    @Override
    public MetaResultSet getTableTypes( final ConnectionHandle connectionHandle ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.TableTypesRequest( connectionHandle.id ) );
            return toResultSet( MetaTableType.class, response );
        } );
    }


    @Override
    public MetaResultSet getTypeInfo( final ConnectionHandle connectionHandle ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.TypeInfoRequest( connectionHandle.id ) );
            return toResultSet( MetaTypeInfo.class, response );
        } );
    }


    @Override
    public MetaResultSet getPrimaryKeys( final ConnectionHandle connectionHandle, final String catalog, final String schema, final String table ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.PrimaryKeysRequest( connectionHandle.id, catalog, schema, table ) );
            return toResultSet( MetaPrimaryKey.class, response );
        } );
    }


    @Override
    public MetaResultSet getImportedKeys( final ConnectionHandle connectionHandle, final String catalog, final String schema, final String table ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.ImportedKeysRequest( connectionHandle.id, catalog, schema, table ) );
            return toResultSet( MetaImportedKey.class, response );
        } );
    }


    @Override
    public MetaResultSet getExportedKeys( final ConnectionHandle connectionHandle, final String catalog, final String schema, final String table ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.ExportedKeysRequest( connectionHandle.id, catalog, schema, table ) );
            return toResultSet( MetaExportedKey.class, response );
        } );
    }


    @Override
    public MetaResultSet getIndexInfo( final ConnectionHandle connectionHandle, final String catalog, final String schema, final String table, final boolean unique, final boolean approximate ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.IndexInfoRequest( connectionHandle.id, catalog, schema, table, unique, approximate ) );
            return toResultSet( MetaIndexInfo.class, response );
        } );
    }


    @Override
    public MetaResultSet getColumns( final ConnectionHandle connectionHandle, final String catalog, final Pat schemaPattern, final Pat tableNamePattern, final Pat columnNamePattern ) {
        return connection.invokeWithRetries( () -> {
            final Service.ResultSetResponse response = service.apply( new Service.ColumnsRequest( connectionHandle.id, catalog, schemaPattern.s, tableNamePattern.s, columnNamePattern.s ) );
            return toResultSet( MetaColumn.class, response );
        } );
    }


    @Override
    public StatementHandle prepare( final ConnectionHandle connectionHandle, final String sql, final long maxRowCount ) {
        return connection.invokeWithRetries( () -> {
            connectionSync( connectionHandle, new ConnectionPropertiesImpl() ); // sync connection state if necessary
            final Service.PrepareResponse response = service.apply( new Service.PrepareRequest( connectionHandle.id, sql, maxRowCount ) );
            return response.statement;
        } );
    }


    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle statementHandle, final String sql, final long maxRowCount, final PrepareCallback callback ) throws NoSuchStatementException {
        // The old semantics were that maxRowCount was also treated as the maximum number of elements in the first Frame of results. A value of -1 would also preserve this, but an
        // explicit (positive) number is easier to follow, IMO.
        return prepareAndExecute( statementHandle, sql, maxRowCount, AvaticaUtils.toSaturatedInt( maxRowCount ), callback );
    }


    @Override
    public ExecuteResult prepareAndExecute( final StatementHandle statementHandle, final String sql, final long maxRowCount, int maxRowsInFirstFrame, final PrepareCallback callback ) throws NoSuchStatementException {
        try {
            return connection.invokeWithRetries( () -> {
                // sync connection state if necessary
                connectionSync( new ConnectionHandle( statementHandle.connectionId ), new ConnectionPropertiesImpl() );
                final Service.ExecuteResponse response;
                try {
                    synchronized ( callback.getMonitor() ) {
                        callback.clear();
                        response = service.apply( new Service.PrepareAndExecuteRequest( statementHandle.connectionId, statementHandle.id, sql, maxRowCount ) );
                        if ( response.missingStatement ) {
                            throw new RuntimeException( new NoSuchStatementException( statementHandle ) ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the NoSuchStatementException
                        }
                        if ( !response.results.isEmpty() ) {
                            final Service.ResultSetResponse result = response.results.get( 0 );
                            callback.assign( result.signature, result.firstFrame, result.updateCount );
                        }
                    }
                    callback.execute();
                    List<MetaResultSet> metaResultSets = new ArrayList<>();
                    for ( Service.ResultSetResponse result : response.results ) {
                        metaResultSets.add( toResultSet( null, result ) );
                    }
                    return new ExecuteResult( metaResultSets );
                } catch ( SQLException e ) {
                    throw new RuntimeException( e ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the SQLException
                }
            } );
        } catch ( RuntimeException e ) {
            Throwable cause = e.getCause();
            if ( cause instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) cause;
            }
            throw e;
        }
    }


    @Override
    public Frame fetch( final StatementHandle statementHandle, final long offset, final int fetchMaxRowCount ) throws NoSuchStatementException, MissingResultsException {
        try {
            return connection.invokeWithRetries( () -> {
                final Service.FetchResponse response = service.apply( new Service.FetchRequest( statementHandle.connectionId, statementHandle.id, offset, fetchMaxRowCount ) );
                if ( response.missingStatement ) {
                    throw new RuntimeException( new NoSuchStatementException( statementHandle ) ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the NoSuchStatementException
                }
                if ( response.missingResults ) {
                    throw new RuntimeException( new MissingResultsException( statementHandle ) ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the MissingResultsException
                }
                return response.frame;
            } );
        } catch ( RuntimeException e ) {
            Throwable cause = e.getCause();
            if ( cause instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) cause;
            } else if ( cause instanceof MissingResultsException ) {
                throw (MissingResultsException) cause;
            }
            throw e;
        }
    }


    @Override
    public ExecuteResult execute( final StatementHandle statementHandle, final List<TypedValue> parameterValues, final long maxRowCount ) throws NoSuchStatementException {
        return execute( statementHandle, parameterValues, AvaticaUtils.toSaturatedInt( maxRowCount ) );
    }


    @Override
    public ExecuteResult execute( final StatementHandle statementHandle, final List<TypedValue> parameterValues, final int maxRowsInFirstFrame ) throws NoSuchStatementException {
        try {
            return connection.invokeWithRetries( () -> {
                final Service.ExecuteResponse response = service.apply( new Service.ExecuteRequest( statementHandle, parameterValues, maxRowsInFirstFrame ) );

                if ( response.missingStatement ) {
                    throw new RuntimeException( new NoSuchStatementException( statementHandle ) ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the NoSuchStatementException
                }

                List<MetaResultSet> metaResultSets = new ArrayList<>();
                for ( Service.ResultSetResponse result : response.results ) {
                    metaResultSets.add( toResultSet( null, result ) );
                }

                return new ExecuteResult( metaResultSets );
            } );
        } catch ( RuntimeException e ) {
            Throwable cause = e.getCause();
            if ( cause instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) cause;
            }
            throw e;
        }
    }


    @Override
    public boolean syncResults( final StatementHandle statementHandle, final QueryState state, final long offset ) throws NoSuchStatementException {
        try {
            return connection.invokeWithRetries( () -> {
                final Service.SyncResultsResponse response = service.apply( new Service.SyncResultsRequest( statementHandle.connectionId, statementHandle.id, state, offset ) );
                if ( response.missingStatement ) {
                    throw new RuntimeException( new NoSuchStatementException( statementHandle ) ); //NOSONAR "squid:S00112" - Justification: The RuntimeException is an envelope for the NoSuchStatementException
                }
                return response.moreResults;
            } );
        } catch ( RuntimeException e ) {
            Throwable cause = e.getCause();
            if ( cause instanceof NoSuchStatementException ) {
                throw (NoSuchStatementException) cause;
            }
            throw e;
        }
    }


    @Override
    public void commit( final ConnectionHandle connectionHandle ) {
        connection.invokeWithRetries( () -> service.apply( new CommitRequest( connectionHandle.id ) ) );
    }


    @Override
    public void rollback( final ConnectionHandle connectionHandle ) {
        connection.invokeWithRetries( () -> service.apply( new RollbackRequest( connectionHandle.id ) ) );
    }


    @Override
    public ExecuteBatchResult prepareAndExecuteBatch( final StatementHandle statementHandle, final List<String> sqlCommands ) {
        return connection.invokeWithRetries( () -> {
            Service.ExecuteBatchResponse response = service.apply( new Service.PrepareAndExecuteBatchRequest( statementHandle.connectionId, statementHandle.id, sqlCommands ) );
            return new ExecuteBatchResult( response.updateCounts );
        } );
    }


    @Override
    public ExecuteBatchResult executeBatch( final StatementHandle statementHandle, final List<List<TypedValue>> parameterValues ) {
        return connection.invokeWithRetries( () -> {
            Service.ExecuteBatchResponse response = service.apply( new Service.ExecuteBatchRequest( statementHandle.connectionId, statementHandle.id, parameterValues ) );
            return new ExecuteBatchResult( response.updateCounts );
        } );
    }

}

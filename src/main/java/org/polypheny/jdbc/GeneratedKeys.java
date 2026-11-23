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

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;
import org.polypheny.prism.GeneratedKeysRequest;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.RelationalFrame;
import org.polypheny.prism.Row;


final class GeneratedKeys {

    static final GeneratedKeysRequest ALL = GeneratedKeysRequest.newBuilder().setAll( true ).build();
    static final Frame EMPTY = Frame.newBuilder().setIsLast( true ).setRelationalFrame( RelationalFrame.getDefaultInstance() ).build();


    private GeneratedKeys() {
    }


    static void requireSupported( PrismInterfaceClient client ) throws SQLFeatureNotSupportedException {
        if ( !client.supportsGeneratedKeys() ) {
            throw new SQLFeatureNotSupportedException( "The connected Polypheny server does not support JDBC generated keys." );
        }
    }


    static GeneratedKeysRequest requested( int flag ) throws SQLException {
        if ( flag == Statement.RETURN_GENERATED_KEYS ) {
            return ALL;
        }
        if ( flag == Statement.NO_GENERATED_KEYS ) {
            return null;
        }
        throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Illegal argument for generated keys: " + flag );
    }


    static GeneratedKeysRequest requested( int[] columnIndexes ) throws SQLException {
        if ( columnIndexes == null || columnIndexes.length == 0 ) {
            return null;
        }
        GeneratedKeysRequest.ColumnIndexes.Builder indexes = GeneratedKeysRequest.ColumnIndexes.newBuilder();
        for ( int index : columnIndexes ) {
            if ( index < 1 ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Generated-key column indexes are one-based." );
            }
            indexes.addIndexes( index );
        }
        return GeneratedKeysRequest.newBuilder().setColumnIndexes( indexes ).build();
    }


    static GeneratedKeysRequest requested( String[] columnNames ) throws SQLException {
        if ( columnNames == null || columnNames.length == 0 ) {
            return null;
        }
        GeneratedKeysRequest.ColumnNames.Builder names = GeneratedKeysRequest.ColumnNames.newBuilder();
        for ( String name : columnNames ) {
            if ( name == null || name.isEmpty() ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.VALUE_ILLEGAL, "Generated-key column names must be nonempty." );
            }
            names.addNames( name );
        }
        return GeneratedKeysRequest.newBuilder().setColumnNames( names ).build();
    }


    static RelationalFrame requireComplete( Frame frame ) throws SQLException {
        if ( !frame.hasRelationalFrame() || !frame.getIsLast() ) {
            throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Generated keys must be a complete relational frame." );
        }
        RelationalFrame keys = frame.getRelationalFrame();
        int columnCount = keys.getColumnMetaCount();
        for ( Row row : keys.getRowsList() ) {
            if ( row.getValuesCount() != columnCount ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.RESULT_TYPE_INVALID, "Generated-key row does not match its column metadata." );
            }
        }
        return keys;
    }


    static Frame combine( List<BatchGeneratedKeys> batch ) throws SQLException {
        if ( batch.isEmpty() ) {
            return EMPTY;
        }
        if ( batch.size() == 1 ) {
            Frame frame = batch.get( 0 ).getKeys();
            requireComplete( frame );
            return frame;
        }

        List<BatchGeneratedKeys> ordered = new ArrayList<>( batch );
        ordered.sort( Comparator.comparingInt( BatchGeneratedKeys::getStatementIndex ) );
        List<ColumnMeta> columns = new ArrayList<>();
        List<List<Integer>> positions = new ArrayList<>();
        List<RelationalFrame> frames = new ArrayList<>();
        for ( BatchGeneratedKeys entry : ordered ) {
            RelationalFrame frame = requireComplete( entry.getKeys() );
            frames.add( frame );
            List<Integer> projection = new ArrayList<>();
            Set<Integer> used = new HashSet<>();
            for ( ColumnMeta column : frame.getColumnMetaList() ) {
                int position = findColumn( columns, column, used );
                if ( position == columns.size() ) {
                    columns.add( column.toBuilder().setColumnIndex( position ).build() );
                } else {
                    ColumnMeta known = columns.get( position );
                    ColumnMeta.Builder merged = known.toBuilder();
                    if ( !known.getEntityName().equals( column.getEntityName() ) || !known.getNamespace().equals( column.getNamespace() ) ) {
                        merged.clearEntityName().clearNamespace();
                    }
                    if ( column.getIsNullable() ) {
                        merged.setIsNullable( true );
                    }
                    if ( known.getIsAutoIncrement() != column.getIsAutoIncrement() ) {
                        merged.clearIsAutoIncrement();
                    }
                    columns.set( position, merged.build() );
                }
                projection.add( position );
                used.add( position );
            }
            positions.add( projection );
        }

        ProtoValue missing = TypedValue.fromNull().serialize();
        RelationalFrame.Builder combined = RelationalFrame.newBuilder();
        for ( int i = 0; i < frames.size(); i++ ) {
            List<Integer> projection = positions.get( i );
            RelationalFrame frame = frames.get( i );
            if ( frame.getRowsCount() > 0 && projection.size() < columns.size() ) {
                for ( int column = 0; column < columns.size(); column++ ) {
                    if ( !projection.contains( column ) ) {
                        columns.set( column, columns.get( column ).toBuilder().setIsNullable( true ).build() );
                    }
                }
            }
            for ( Row row : frame.getRowsList() ) {
                List<ProtoValue> values = new ArrayList<>( Collections.nCopies( columns.size(), missing ) );
                for ( int column = 0; column < row.getValuesCount(); column++ ) {
                    values.set( projection.get( column ), row.getValues( column ) );
                }
                combined.addRows( Row.newBuilder().addAllValues( values ) );
            }
        }
        return Frame.newBuilder().setIsLast( true ).setRelationalFrame( combined.addAllColumnMeta( columns ) ).build();
    }


    private static int findColumn( List<ColumnMeta> columns, ColumnMeta candidate, Set<Integer> used ) {
        for ( int i = 0; i < columns.size(); i++ ) {
            ColumnMeta column = columns.get( i );
            if ( !used.contains( i )
                    && column.getColumnName().equals( candidate.getColumnName() )
                    && column.getColumnLabel().equals( candidate.getColumnLabel() )
                    && column.getTypeMeta().equals( candidate.getTypeMeta() ) ) {
                return i;
            }
        }
        return columns.size();
    }

}

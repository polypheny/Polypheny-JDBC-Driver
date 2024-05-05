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

package org.polypheny.jdbc.meta;

import java.util.Comparator;
import org.polypheny.prism.ClientInfoPropertyMeta;
import org.polypheny.prism.Column;
import org.polypheny.prism.Function;
import org.polypheny.prism.Namespace;
import org.polypheny.prism.Table;
import org.polypheny.prism.Type;
import org.polypheny.jdbc.utils.TypedValueUtils;

public class MetaResultSetComparators {

    public static final Comparator<Type> TYPE_INFO_COMPARATOR = Comparator
            .comparing( t -> TypedValueUtils.getJdbcTypeFromPolyTypeName( t.getTypeName() ) );
    public static final Comparator<Namespace> NAMESPACE_COMPARATOR = Comparator
            .comparing( Namespace::getNamespaceName );
    public static final Comparator<GenericMetaContainer> PRIMARY_KEY_COMPARATOR = Comparator
            .comparing( g -> (String) (g.getValue( 2 )) );
    public static final Comparator<GenericMetaContainer> INDEX_COMPARATOR = Comparator
            .comparing( ( GenericMetaContainer g ) -> (Boolean) (g.getValue( 2 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (int) (g.getValue( 7 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 3 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (Integer) (g.getValue( 4 )) );
    public static final Comparator<GenericMetaContainer> IMPORTED_KEYS_COMPARATOR = Comparator
            .comparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 0 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 1 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (Integer) (g.getValue( 5 )) );
    public static final Comparator<GenericMetaContainer> EXPORTED_KEYS_COMPARATOR = Comparator
            .comparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 4 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 5 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 6 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (Integer) (g.getValue( 8 )) );
    // Both use the same ordering according to JDBC standard
    public static final Comparator<GenericMetaContainer> CROSS_REFERENCE_COMPARATOR = EXPORTED_KEYS_COMPARATOR;
    public static final Comparator<Function> FUNCTION_COMPARATOR = Comparator
            .comparing( Function::getName );
    public static final Comparator<Column> COLUMN_COMPARATOR = Comparator
            .comparing( Column::getNamespaceName )
            .thenComparing( Column::getTableName )
            .thenComparing( Column::getColumnIndex );
    public static final Comparator<Table> TABLE_COMPARATOR = Comparator
            .comparing( Table::getTableType )
            .thenComparing( Table::getNamespaceName )
            .thenComparing( Table::getTableName );
    public static final Comparator<Column> PSEUDO_COLUMN_COMPARATOR = Comparator
            .comparing( Column::getNamespaceName )
            .thenComparing( Column::getTableName )
            .thenComparing( Column::getColumnName );
    public static final Comparator<ClientInfoPropertyMeta> CLIENT_INFO_PROPERTY_COMPARATOR = Comparator
            .comparing( ClientInfoPropertyMeta::getKey );
    public static final Comparator<GenericMetaContainer> TABLE_PRIVILEGE_COMPARATOR = Comparator
            .comparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 0 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 1 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 2 )) )
            .thenComparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 5 )) );
    public static final Comparator<GenericMetaContainer> COLUMN_PRIVILEGE_COMPARATOR = Comparator
            .comparing( ( GenericMetaContainer g ) -> (String) (g.getValue( 6 )) );

}

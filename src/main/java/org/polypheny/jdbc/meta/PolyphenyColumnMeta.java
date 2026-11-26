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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.types.PolyEdge;
import org.polypheny.jdbc.types.PolyGraph;
import org.polypheny.jdbc.types.PolyNode;
import org.polypheny.jdbc.types.PolyPath;
import org.polypheny.jdbc.types.ProtoToJdbcTypeMap;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.ProtoPolyType;

public class PolyphenyColumnMeta {

    @Getter
    private final int ordinal;
    @Getter
    private final boolean autoIncrement;
    @Getter
    private final boolean caseSensitive;
    @Getter
    private final boolean searchable;
    @Getter
    private final boolean currency;
    @Getter
    private final int nullable;
    @Getter
    private final boolean signed;
    @Getter
    private final int displaySize;
    @Getter
    private final String columnLabel;
    @Getter
    private final String columnName;
    @Getter
    private final String namespace;
    @Getter
    private final int precision;
    @Getter
    private final int scale;
    @Getter
    private final String tableName;
    @Getter
    private final String catalogName;
    @Getter
    private final boolean readOnly;
    @Getter
    private final boolean writable;
    @Getter
    private final boolean definitelyWritable;
    @Getter
    private final String columnClassName;
    @Getter
    private final int sqlType;
    @Getter
    private final String polyphenyFieldTypeName;


    //column = field
    public PolyphenyColumnMeta( ColumnMeta protoColumnMeta ) {
        ProtoPolyType type = protoColumnMeta.getTypeMeta().getProtoValueType();
        this.ordinal = protoColumnMeta.getColumnIndex();
        this.autoIncrement = protoColumnMeta.getIsAutoIncrement();
        this.caseSensitive = true;
        this.searchable = false;
        this.currency = false;
        this.nullable = protoColumnMeta.getIsNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
        this.signed = isSigned( type );
        this.displaySize = protoColumnMeta.getLength();
        this.columnLabel = protoColumnMeta.getColumnLabel();
        this.columnName = protoColumnMeta.getColumnName();
        this.namespace = schemaName( protoColumnMeta );
        this.precision = protoColumnMeta.getPrecision();
        this.scale = type == ProtoPolyType.DECIMAL ? protoColumnMeta.getScale() : 0;
        // table = entity
        this.tableName = protoColumnMeta.getEntityName();
        this.catalogName = "";
        this.readOnly = false;
        this.writable = false;
        this.definitelyWritable = false;
        this.columnClassName = getColumnClassName( type );
        if ( type == ProtoPolyType.USER_DEFINED_TYPE ) {
            //TODO: This is required once user defined types are introduced. Depending on their implementation this might even become obsolete.
            throw new NotImplementedException( "Struct types not implemented yet" );
        }
        if ( type == ProtoPolyType.ARRAY ) {
            ProtoPolyType elementType = protoColumnMeta.getTypeMeta().getArrayMeta().getElementType().getProtoValueType();
            this.sqlType = JDBCType.ARRAY.getVendorTypeNumber();
            this.polyphenyFieldTypeName = elementType.name();
            return;
        }
        this.sqlType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( type );
        this.polyphenyFieldTypeName = type.name();
    }


    public static String schemaName( ColumnMeta columnMeta ) {
        return columnMeta.getNamespace().isEmpty() ? columnMeta.getSchemaName() : columnMeta.getNamespace();
    }


    private static boolean isSigned( ProtoPolyType type ) {
        switch ( type ) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case DECIMAL:
            case REAL:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }


    private static String getColumnClassName( ProtoPolyType type ) {
        switch ( type ) {
            case BOOLEAN:
                return Boolean.class.getName();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Integer.class.getName();
            case BIGINT:
                return Long.class.getName();
            case DECIMAL:
                return BigDecimal.class.getName();
            case REAL:
                return Float.class.getName();
            case FLOAT:
            case DOUBLE:
                return Double.class.getName();
            case DATE:
                return Date.class.getName();
            case TIME:
                return Time.class.getName();
            case TIMESTAMP:
                return Timestamp.class.getName();
            case CHAR:
            case VARCHAR:
            case TEXT:
                return String.class.getName();
            case BINARY:
            case VARBINARY:
                return byte[].class.getName();
            case NODE:
                return PolyNode.class.getName();
            case EDGE:
                return PolyEdge.class.getName();
            case PATH:
                return PolyPath.class.getName();
            case GRAPH:
                return PolyGraph.class.getName();
            default:
                return "";
        }
    }


    // Only there so constructor remains hidden to indicate that it shouldn't be used for anything else
    public static PolyphenyColumnMeta fromSpecification( int ordinal, String columnLabel, String entityName, int jdcType ) {
        return new PolyphenyColumnMeta( ordinal, columnLabel, entityName, jdcType );
    }


    /* This constructor is used exclusively to create metadata for the responses of the meta endpoint since these must be
     * represented as ResultSets.
     */
    private PolyphenyColumnMeta( int ordinal, String columnLabel, String entityName, int jdbcType ) {
        this.ordinal = ordinal;
        this.autoIncrement = false;
        this.caseSensitive = true;
        this.searchable = false;
        this.currency = false;
        this.nullable = ResultSetMetaData.columnNullable;
        this.signed = false;
        this.displaySize = -1;
        this.columnLabel = columnLabel;
        this.columnName = columnLabel;
        this.namespace = null;
        this.precision = -1;
        this.scale = 1;
        this.tableName = entityName;
        this.catalogName = "";
        this.readOnly = false;
        this.writable = false;
        this.definitelyWritable = false;
        this.columnClassName = "";
        this.sqlType = jdbcType;
        this.polyphenyFieldTypeName = "";
    }

}

package org.polypheny.jdbc.meta;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.types.ProtoToJdbcTypeMap;

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
        this.ordinal = protoColumnMeta.getColumnIndex();
        this.autoIncrement = false;
        this.caseSensitive = true;
        this.searchable = false;
        this.currency = false;
        this.nullable = protoColumnMeta.getIsNullable() ? ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
        this.signed = false;
        this.displaySize = protoColumnMeta.getLength();
        this.columnLabel = protoColumnMeta.getColumnLabel();
        this.columnName = protoColumnMeta.getColumnName();
        this.namespace = protoColumnMeta.getNamespace();
        this.precision = protoColumnMeta.getPrecision();
        this.scale = 1;
        // table = entity
        this.tableName = protoColumnMeta.getEntityName();
        this.catalogName = "";
        this.readOnly = false;
        this.writable = false;
        this.definitelyWritable = false;
        this.columnClassName = "";
        if ( protoColumnMeta.getTypeMeta().getProtoValueType() == ProtoPolyType.USER_DEFINED_TYPE ) {
            //TODO handle structured meta
            throw new NotImplementedException( "Struct types not implemented yet" );
        }
        if ( protoColumnMeta.getTypeMeta().getProtoValueType() == ProtoPolyType.ARRAY ) {
            ProtoPolyType type = protoColumnMeta.getTypeMeta().getArrayMeta().getElementType().getProtoValueType();
            this.sqlType = JDBCType.ARRAY.getVendorTypeNumber();
            this.polyphenyFieldTypeName = type.name();
            return;
        }
        ProtoPolyType type = protoColumnMeta.getTypeMeta().getProtoValueType();
        this.sqlType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( type );
        this.polyphenyFieldTypeName = type.name();
    }


    // Only there so constructor remains hidden to indicate that it shouldn't be used for anything else
    public static PolyphenyColumnMeta fromSpecification( int ordinal, String columnLabel, String entityName, int jdcType ) {
        return new PolyphenyColumnMeta( ordinal, columnLabel, entityName, jdcType );
    }


    /* This constructor is used exclusively to create metadata for the responses of the meta endpoint since these must be
     * represented as resultsets.
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

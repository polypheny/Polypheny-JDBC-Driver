package org.polypheny.jdbc;

import java.sql.ResultSetMetaData;
import lombok.Getter;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ColumnMeta;
import org.polypheny.jdbc.proto.ProtoValueType;
import org.polypheny.jdbc.types.ProtoToJdbcTypeMap;
import org.polypheny.jdbc.types.ProtoToPolyTypeNameMap;

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
        this.nullable = protoColumnMeta.getIsNullable() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
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
        if (protoColumnMeta.getTypeMeta().getProtoValueType() == ProtoValueType.PROTO_VALUE_TYPE_STRUCTURED) {
            throw new NotImplementedException("Struct types not implemented yet");
        } else {
            this.sqlType = ProtoToJdbcTypeMap.getJdbcTypeFromProto(protoColumnMeta.getTypeMeta().getProtoValueType());
        }
        this.polyphenyFieldTypeName = ProtoToPolyTypeNameMap.getPolyTypeNameFromProto( protoColumnMeta.getTypeMeta().getProtoValueType() );
    }
}

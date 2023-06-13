package org.polypheny.jdbc;

import java.sql.ResultSetMetaData;
import lombok.Getter;
import org.polypheny.jdbc.proto.ColumnMeta;
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
    private final String schemaName;
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
    private final String databaseTypeName;


    public PolyphenyColumnMeta( ColumnMeta protoColumnMeta ) {
        this.ordinal = protoColumnMeta.getColumnIndex();
        this.autoIncrement = false;
        this.caseSensitive = true;
        this.searchable = false;
        this.currency = false;
        //TODO TH: convert nullability
        this.nullable = protoColumnMeta.getIsNullable() ? ResultSetMetaData.columnNoNulls : ResultSetMetaData.columnNullable;
        ;
        this.signed = false;
        this.displaySize = protoColumnMeta.getDisplaySize();
        this.columnLabel = protoColumnMeta.getColumnLabel();
        this.columnName = protoColumnMeta.getColumnName();
        this.schemaName = "";
        this.precision = protoColumnMeta.getPrecision();
        this.scale = 1;
        this.tableName = protoColumnMeta.getTableName();
        this.catalogName = "";
        this.readOnly = false;
        this.writable = false;
        this.definitelyWritable = false;
        this.columnClassName = "";
        this.sqlType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( protoColumnMeta.getProtoValueType() );
        this.databaseTypeName = ProtoToPolyTypeNameMap.getPolyTypeNameFromProto( protoColumnMeta.getProtoValueType() );
    }
}

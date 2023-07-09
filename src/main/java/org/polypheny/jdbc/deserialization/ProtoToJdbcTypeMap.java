package org.polypheny.jdbc.deserialization;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValueType;

public class ProtoToJdbcTypeMap {
    private static final Map<ProtoValueType, Integer> PROTO_TYPE_TO_JDBC =
            ImmutableMap.<ProtoValueType, Integer>builder()
                    .put( ProtoValueType.PROTO_VALUE_TYPE_BOOLEAN, Types.BOOLEAN)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTEGER, Types.INTEGER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_VARCHAR, Types.VARCHAR)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_JSON, Types.VARCHAR)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_DATE, Types.DATE)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_TIME, Types.TIME)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_TIMESTAMP, Types.TIMESTAMP)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_NULL, Types.NULL)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_DECIMAL, Types.DECIMAL)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_ANY, Types.JAVA_OBJECT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_CHAR, Types.CHAR)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_BINARY, Types.BINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_VARBINARY, Types.VARBINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_FILE, Types.BINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_IMAGE, Types.BINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_VIDEO, Types.BINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_AUDIO, Types.BINARY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_TINYINT, Types.TINYINT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_SMALLINT, Types.SMALLINT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_BIGINT, Types.BIGINT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_REAL, Types.REAL)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_DOUBLE, Types.DOUBLE)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_SYMBOL, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_YEAR, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_YEAR_MONTH, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_MONTH, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_DAY, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_DAY_HOUR, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_DAY_MINUTE, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_DAY_SECOND, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_HOUR, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_HOUR_MINUTE, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_HOUR_SECOND, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_MINUTE, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_MINUTE_SECOND, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_INTERVAL_SECOND, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_TIME_WITH_LOCAL_TIME_ZONE, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_TIMESTAMP_WITH_LOCAL_TIME_ZONE, Types.OTHER)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_FLOAT, Types.FLOAT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_MULTISET, Types.ARRAY)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_DISTINCT, Types.DISTINCT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_STRUCTURED, Types.STRUCT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_ROW, Types.STRUCT)
                    .put( ProtoValueType.PROTO_VALUE_TYPE_COLUMN_LIST, Types.OTHER + 2)
                    .build();


    public static int getJdbcTypeFromProto(ProtoValueType protoValueType) {
        Integer jdbcType = PROTO_TYPE_TO_JDBC.get( protoValueType );
        if (jdbcType == null) {
            throw new IllegalArgumentException("Invalid proto value type.");
        }
        return jdbcType;
    }
}

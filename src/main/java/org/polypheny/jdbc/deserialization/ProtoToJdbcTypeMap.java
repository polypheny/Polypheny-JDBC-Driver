package org.polypheny.jdbc.deserialization;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValue;

public class ProtoToJdbcTypeMap {

    private static final Map<ProtoValue.ProtoValueType, Integer> PROTO_TYPE_TO_JDBC =
            ImmutableMap.<ProtoValue.ProtoValueType, Integer>builder()
                    .put( ProtoValue.ProtoValueType.BOOLEAN, Types.BOOLEAN )
                    .put( ProtoValue.ProtoValueType.INTEGER, Types.INTEGER )
                    .put( ProtoValue.ProtoValueType.VARCHAR, Types.VARCHAR )
                    .put( ProtoValue.ProtoValueType.DATE, Types.DATE )
                    .put( ProtoValue.ProtoValueType.TIME, Types.TIME )
                    .put( ProtoValue.ProtoValueType.TIMESTAMP, Types.TIMESTAMP )
                    .put( ProtoValue.ProtoValueType.NULL, Types.NULL )
                    .put( ProtoValue.ProtoValueType.DECIMAL, Types.DECIMAL )
                    .put( ProtoValue.ProtoValueType.BINARY, Types.BINARY )
                    .put( ProtoValue.ProtoValueType.FILE, Types.BINARY )
                    .put( ProtoValue.ProtoValueType.BIGINT, Types.BIGINT )
                    .put( ProtoValue.ProtoValueType.DOUBLE, Types.DOUBLE )
                    .put( ProtoValue.ProtoValueType.SYMBOL, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_YEAR, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_YEAR_MONTH, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_MONTH, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_DAY, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_DAY_HOUR, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_DAY_MINUTE, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_DAY_SECOND, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_HOUR, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_HOUR_MINUTE, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_HOUR_SECOND, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_MINUTE, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_MINUTE_SECOND, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.INTERVAL_SECOND, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.FLOAT, Types.FLOAT )
                    .put( ProtoValue.ProtoValueType.ARRAY, Types.ARRAY )
                    .put( ProtoValue.ProtoValueType.MAP, Types.OTHER )
                    .put( ProtoValue.ProtoValueType.DOCUMENT, Types.STRUCT )
                    .put( ProtoValue.ProtoValueType.GRAPH, Types.JAVA_OBJECT )
                    .put( ProtoValue.ProtoValueType.NODE, Types.JAVA_OBJECT )
                    .put( ProtoValue.ProtoValueType.EDGE, Types.JAVA_OBJECT )
                    .put( ProtoValue.ProtoValueType.PATH, Types.JAVA_OBJECT )
                    .put( ProtoValue.ProtoValueType.FILE, Types.BINARY )
                    .put( ProtoValue.ProtoValueType.USER_DEFINED_TYPE, Types.OTHER )
                    .build();


    public static int getJdbcTypeFromProto( ProtoValue.ProtoValueType protoValueType ) {
        Integer jdbcType = PROTO_TYPE_TO_JDBC.get( protoValueType );
        if ( jdbcType == null ) {
            throw new IllegalArgumentException( "Invalid proto value type." );
        }
        return jdbcType;
    }

}

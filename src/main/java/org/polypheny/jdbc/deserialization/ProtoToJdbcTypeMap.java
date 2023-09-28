package org.polypheny.jdbc.deserialization;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;
import org.polypheny.jdbc.jdbctypes.ExtraPolyTypes;

public class ProtoToJdbcTypeMap {

    private static final Map<ProtoValue.ProtoValueType, Integer> PROTO_TYPE_TO_JDBC =
            ImmutableMap.<ProtoValue.ProtoValueType, Integer>builder()
                    .put( ProtoValueType.BOOLEAN, Types.BOOLEAN )
                    .put( ProtoValueType.TINYINT, Types.TINYINT )
                    .put( ProtoValueType.SMALLINT, Types.SMALLINT )
                    .put( ProtoValueType.INTEGER, Types.INTEGER )
                    .put( ProtoValueType.BIGINT, Types.BIGINT )
                    .put( ProtoValueType.DECIMAL, Types.DECIMAL )
                    .put( ProtoValueType.REAL, Types.REAL )
                    .put( ProtoValueType.FLOAT, Types.FLOAT )
                    .put( ProtoValueType.DOUBLE, Types.DOUBLE )
                    .put( ProtoValueType.DATE, Types.DATE )
                    .put( ProtoValueType.TIME, Types.TIME )
                    .put( ProtoValueType.TIME_WITH_LOCAL_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE )
                    .put( ProtoValueType.TIMESTAMP, Types.TIMESTAMP )
                    .put( ProtoValueType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE )
                    .put( ProtoValueType.INTERVAL_SECOND, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_MINUTE_SECOND, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_MINUTE, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_HOUR_SECOND, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_HOUR_MINUTE, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_HOUR, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_DAY_SECOND, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_DAY_MINUTE, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_DAY_HOUR, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_DAY, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_MONTH, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_YEAR_MONTH, Types.OTHER )
                    .put( ProtoValueType.INTERVAL_YEAR, Types.OTHER )
                    .put( ProtoValueType.CHAR, Types.CHAR )
                    .put( ProtoValueType.VARCHAR, Types.VARCHAR )
                    .put( ProtoValueType.BINARY, Types.BINARY )
                    .put( ProtoValueType.VARBINARY, Types.VARBINARY )
                    .put( ProtoValueType.NULL, Types.NULL )
                    .put( ProtoValueType.ARRAY, Types.ARRAY)
                    .put( ProtoValueType.MAP, Types.OTHER )
                    .put( ProtoValueType.DOCUMENT, Types.STRUCT )
                    .put( ProtoValueType.GRAPH, Types.JAVA_OBJECT )
                    .put( ProtoValueType.NODE, Types.JAVA_OBJECT )
                    .put( ProtoValueType.EDGE, Types.JAVA_OBJECT )
                    .put( ProtoValueType.PATH, Types.JAVA_OBJECT )
                    .put( ProtoValueType.IMAGE, Types.BINARY )
                    .put( ProtoValueType.VIDEO, Types.BINARY )
                    .put( ProtoValueType.AUDIO, Types.BINARY )
                    .put( ProtoValueType.FILE, Types.BINARY )
                    .put( ProtoValueType.DISTINCT, Types.DISTINCT )
                    .put( ProtoValueType.STRUCTURED, Types.STRUCT )
                    .put( ProtoValueType.OTHER, Types.OTHER )
                    .put( ProtoValueType.CURSOR, Types.REF_CURSOR )
                    .put( ProtoValueType.COLUMN_LIST, Types.OTHER + 2 )
                    .put( ProtoValueType.DYNAMIC_STAR, Types.JAVA_OBJECT )
                    .put( ProtoValueType.GEOMETRY, ExtraPolyTypes.GEOMETRY )
                    .put( ProtoValueType.SYMBOL, Types.OTHER )
                    .put( ProtoValueType.JSON, Types.VARCHAR )
                    .put( ProtoValueType.MULTISET, Types.ARRAY )
                    .put( ProtoValueType.ANY, Types.JAVA_OBJECT )
                    .put( ProtoValueType.USER_DEFINED_TYPE, Types.OTHER )
                    .put( ProtoValueType.ROW, Types.ROWID)
                    .build();


    public static int getJdbcTypeFromProto( ProtoValue.ProtoValueType protoValueType ) {
        Integer jdbcType = PROTO_TYPE_TO_JDBC.get( protoValueType );
        if ( jdbcType == null ) {
            throw new IllegalArgumentException( "Invalid proto value type." );
        }
        return jdbcType;
    }

}

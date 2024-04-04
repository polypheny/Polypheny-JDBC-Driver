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

package org.polypheny.jdbc.types;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class ProtoToJdbcTypeMap {

    private static final Map<ProtoPolyType, Integer> PROTO_TYPE_TO_JDBC =
            ImmutableMap.<ProtoPolyType, Integer>builder()
                    .put( ProtoPolyType.BOOLEAN, Types.BOOLEAN )
                    .put( ProtoPolyType.TINYINT, Types.TINYINT )
                    .put( ProtoPolyType.SMALLINT, Types.SMALLINT )
                    .put( ProtoPolyType.INTEGER, Types.INTEGER )
                    .put( ProtoPolyType.BIGINT, Types.BIGINT )
                    .put( ProtoPolyType.DECIMAL, Types.DECIMAL )
                    .put( ProtoPolyType.REAL, Types.REAL )
                    .put( ProtoPolyType.FLOAT, Types.FLOAT )
                    .put( ProtoPolyType.DOUBLE, Types.DOUBLE )
                    .put( ProtoPolyType.DATE, Types.DATE )
                    .put( ProtoPolyType.TIME, Types.TIME )
                    .put( ProtoPolyType.TIME_WITH_LOCAL_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE )
                    .put( ProtoPolyType.TIMESTAMP, Types.TIMESTAMP )
                    .put( ProtoPolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, Types.TIMESTAMP_WITH_TIMEZONE )
                    .put( ProtoPolyType.CHAR, Types.CHAR )
                    .put( ProtoPolyType.VARCHAR, Types.VARCHAR )
                    .put( ProtoPolyType.TEXT, Types.VARCHAR ) // TODO is Types.VARCHAR correct?
                    .put( ProtoPolyType.BINARY, Types.BINARY )
                    .put( ProtoPolyType.VARBINARY, Types.VARBINARY )
                    .put( ProtoPolyType.NULL, Types.NULL )
                    .put( ProtoPolyType.ARRAY, Types.ARRAY )
                    .put( ProtoPolyType.MAP, Types.OTHER )
                    .put( ProtoPolyType.DOCUMENT, Types.STRUCT )
                    .put( ProtoPolyType.GRAPH, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.NODE, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.EDGE, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.PATH, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.IMAGE, Types.BINARY )
                    .put( ProtoPolyType.VIDEO, Types.BINARY )
                    .put( ProtoPolyType.AUDIO, Types.BINARY )
                    .put( ProtoPolyType.FILE, Types.BINARY )
                    .put( ProtoPolyType.DISTINCT, Types.DISTINCT )
                    .put( ProtoPolyType.STRUCTURED, Types.STRUCT )
                    .put( ProtoPolyType.OTHER, Types.OTHER )
                    .put( ProtoPolyType.CURSOR, Types.REF_CURSOR )
                    .put( ProtoPolyType.COLUMN_LIST, Types.OTHER + 2 )
                    .put( ProtoPolyType.DYNAMIC_STAR, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.GEOMETRY, ExtraPolyTypes.GEOMETRY )
                    .put( ProtoPolyType.SYMBOL, Types.OTHER )
                    .put( ProtoPolyType.JSON, Types.VARCHAR )
                    .put( ProtoPolyType.MULTISET, Types.ARRAY )
                    .put( ProtoPolyType.ANY, Types.JAVA_OBJECT )
                    .put( ProtoPolyType.USER_DEFINED_TYPE, Types.OTHER )
                    .put( ProtoPolyType.ROW, Types.ROWID )
                    .put(ProtoPolyType.INTERVAL_SECOND, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_MINUTE_SECOND, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_MINUTE, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_HOUR_SECOND, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_HOUR_MINUTE, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_HOUR, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_DAY_SECOND, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_DAY_MINUTE, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_DAY_HOUR, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_DAY, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_MONTH, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_YEAR_MONTH, Types.OTHER)
                    .put(ProtoPolyType.INTERVAL_YEAR, Types.OTHER)
                    .build();


    public static int getJdbcTypeFromProto( ProtoPolyType ProtoPolyType ) {
        Integer jdbcType = PROTO_TYPE_TO_JDBC.get( ProtoPolyType );
        if ( jdbcType == null ) {
            throw new IllegalArgumentException( "Invalid proto value type: " + ProtoPolyType.name() + "." );
        }
        return jdbcType;
    }
}

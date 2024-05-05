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

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.prism.ProtoPolyType;

public class ProtoToJdbcTypeMap {

    private static final Map<ProtoPolyType, Integer> PROTO_TYPE_TO_JDBC = new HashMap<>();


    static {
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.BOOLEAN, Types.BOOLEAN );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.TINYINT, Types.TINYINT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.SMALLINT, Types.SMALLINT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.INTEGER, Types.INTEGER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.BIGINT, Types.BIGINT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DECIMAL, Types.DECIMAL );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.REAL, Types.REAL );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.FLOAT, Types.FLOAT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DOUBLE, Types.DOUBLE );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DATE, Types.DATE );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.TIME, Types.TIME );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.TIMESTAMP, Types.TIMESTAMP );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.INTERVAL, Types.OTHER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.CHAR, Types.CHAR );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.VARCHAR, Types.VARCHAR );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.TEXT, Types.VARCHAR );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.BINARY, Types.BINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.VARBINARY, Types.VARBINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.NULL, Types.NULL );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.ARRAY, Types.ARRAY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.MAP, Types.OTHER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DOCUMENT, Types.STRUCT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.GRAPH, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.NODE, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.EDGE, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.PATH, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.IMAGE, Types.BINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.VIDEO, Types.BINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.AUDIO, Types.BINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.FILE, Types.BINARY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DISTINCT, Types.DISTINCT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.STRUCTURED, Types.STRUCT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.OTHER, Types.OTHER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.CURSOR, Types.REF_CURSOR );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.COLUMN_LIST, Types.OTHER + 2 );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.DYNAMIC_STAR, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.GEOMETRY, ExtraPolyTypes.GEOMETRY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.SYMBOL, Types.OTHER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.JSON, Types.VARCHAR );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.MULTISET, Types.ARRAY );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.ANY, Types.JAVA_OBJECT );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.USER_DEFINED_TYPE, Types.OTHER );
        PROTO_TYPE_TO_JDBC.put( ProtoPolyType.ROW, Types.ROWID );
    }


    public static int getJdbcTypeFromProto( ProtoPolyType ProtoPolyType ) {
        Integer jdbcType = PROTO_TYPE_TO_JDBC.get( ProtoPolyType );
        if ( jdbcType == null ) {
            throw new IllegalArgumentException( "Invalid proto value type: " + ProtoPolyType.name() + "." );
        }
        return jdbcType;
    }

}

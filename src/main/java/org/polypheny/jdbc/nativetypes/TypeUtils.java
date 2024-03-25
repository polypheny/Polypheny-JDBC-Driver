/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.jdbc.nativetypes;


import static org.polypheny.db.protointerface.proto.ProtoPolyType.AUDIO;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.BIGINT;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.DATE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.DECIMAL;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.DOUBLE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.FILE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.FLOAT;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.IMAGE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.INTEGER;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.REAL;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.SMALLINT;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.TIME;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.TIMESTAMP;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.TIME_WITH_LOCAL_TIME_ZONE;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.TINYINT;
import static org.polypheny.db.protointerface.proto.ProtoPolyType.VIDEO;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class TypeUtils {

    public static final List<ProtoPolyType> DATETIME_TYPES = ImmutableList.of( DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE );
    public static final List<ProtoPolyType> INT_TYPES = ImmutableList.of( TINYINT, SMALLINT, INTEGER, BIGINT );

    public static final List<ProtoPolyType> EXACT_TYPES = combine( INT_TYPES, ImmutableList.of( DECIMAL ) );

    public static final List<ProtoPolyType> APPROX_TYPES = ImmutableList.of( FLOAT, REAL, DOUBLE );

    public static final List<ProtoPolyType> NUMERIC_TYPES = combine( EXACT_TYPES, APPROX_TYPES );

    public static final List<ProtoPolyType> FRACTIONAL_TYPES = combine( APPROX_TYPES, ImmutableList.of( DECIMAL ) );


    public static final List<ProtoPolyType> BLOB_TYPES = ImmutableList.of( FILE, AUDIO, IMAGE, VIDEO );


    private static List<ProtoPolyType> combine( List<ProtoPolyType> list0, List<ProtoPolyType> list1 ) {
        return ImmutableList.<ProtoPolyType>builder()
                .addAll( list0 )
                .addAll( list1 )
                .build();
    }

}

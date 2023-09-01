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

import static org.polypheny.jdbc.proto.ProtoValue.ProtoValueType.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.polypheny.jdbc.proto.ProtoValue;

public class TypeUtils {
    public static final List<ProtoValue.ProtoValueType> DATETIME_TYPES = ImmutableList.of( DATE, TIME, TIME_WITH_LOCAL_TIME_ZONE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE );
    public static final List<ProtoValue.ProtoValueType> INT_TYPES = ImmutableList.of( TINYINT, SMALLINT, INTEGER, BIGINT );

    public static final List<ProtoValue.ProtoValueType> EXACT_TYPES = combine( INT_TYPES, ImmutableList.of( DECIMAL ) );

    public static final List<ProtoValue.ProtoValueType> APPROX_TYPES = ImmutableList.of( FLOAT, REAL, DOUBLE );

    public static final List<ProtoValue.ProtoValueType> NUMERIC_TYPES = combine( EXACT_TYPES, APPROX_TYPES );

    public static final List<ProtoValue.ProtoValueType> FRACTIONAL_TYPES = combine( APPROX_TYPES, ImmutableList.of( DECIMAL ) );

    public static final Set<ProtoValue.ProtoValueType> YEAR_INTERVAL_TYPES =
            Sets.immutableEnumSet(
                    INTERVAL_YEAR,
                    INTERVAL_YEAR_MONTH,
                    INTERVAL_MONTH );

    public static final Set<ProtoValue.ProtoValueType> DAY_INTERVAL_TYPES =
            Sets.immutableEnumSet(
                    INTERVAL_DAY,
                    INTERVAL_DAY_HOUR,
                    INTERVAL_DAY_MINUTE,
                    INTERVAL_DAY_SECOND,
                    INTERVAL_HOUR,
                    INTERVAL_HOUR_MINUTE,
                    INTERVAL_HOUR_SECOND,
                    INTERVAL_MINUTE,
                    INTERVAL_MINUTE_SECOND,
                    INTERVAL_SECOND );

    public static final Set<ProtoValue.ProtoValueType> INTERVAL_TYPES = Sets.immutableEnumSet( Iterables.concat( YEAR_INTERVAL_TYPES, DAY_INTERVAL_TYPES ) );

    public static final List<ProtoValue.ProtoValueType> BLOB_TYPES = ImmutableList.of( FILE, AUDIO, IMAGE, VIDEO );

    private static List<ProtoValue.ProtoValueType> combine( List<ProtoValue.ProtoValueType> list0, List<ProtoValue.ProtoValueType> list1 ) {
        return ImmutableList.<ProtoValue.ProtoValueType>builder()
                .addAll( list0 )
                .addAll( list1 )
                .build();
    }
}

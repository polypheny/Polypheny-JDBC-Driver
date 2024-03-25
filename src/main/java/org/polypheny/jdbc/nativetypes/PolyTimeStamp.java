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

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.TimeZone;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;

public class PolyTimeStamp extends PolyTemporal {

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    @Getter
    public Long milliSinceEpoch; // normalized to utz


    public PolyTimeStamp( Long milliSinceEpoch ) {
        super( ProtoPolyType.TIMESTAMP );
        this.milliSinceEpoch = milliSinceEpoch;
    }


    public static PolyTimeStamp of( Number number ) {
        return new PolyTimeStamp( number.longValue() );
    }


    public static PolyTimeStamp ofNullable( Number number ) {
        return number == null ? null : of( number );
    }


    public static PolyTimeStamp ofNullable( Time value ) {
        return value == null ? null : PolyTimeStamp.of( value );
    }


    public static PolyTimeStamp of( long value ) {
        return new PolyTimeStamp( value );
    }


    public static PolyTimeStamp of( Long value ) {
        return new PolyTimeStamp( value );
    }


    public static PolyTimeStamp of( Timestamp value ) {
        if ( value == null ) {
            return null;
        }
        long time = value.getTime();
        time += LOCAL_TZ.getOffset( time );
        return new PolyTimeStamp( time );
    }


    public static PolyTimeStamp of( Date date ) {
        return new PolyTimeStamp( date.getTime() );
    }


    public Timestamp asSqlTimestamp() {
        return new Timestamp( milliSinceEpoch );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return Long.compare( milliSinceEpoch, o.asTimeStamp().milliSinceEpoch );
    }

}

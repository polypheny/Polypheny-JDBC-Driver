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
import java.util.TimeZone;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.db.protointerface.proto.ProtoTime;
import org.polypheny.db.protointerface.proto.ProtoValue;

public class PolyTime extends PolyTemporal {

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();
    public Integer ofDay;

    public ProtoTime.TimeUnit timeUnit;


    public PolyTime( int ofDay, ProtoTime.TimeUnit timeUnit ) {
        super( ProtoPolyType.TIME );
        this.ofDay = ofDay;
        this.timeUnit = timeUnit;
    }


    public static PolyTime of( Number value ) {
        return new PolyTime( value.intValue(), ProtoTime.TimeUnit.MILLISECOND );
    }


    public static PolyTime ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    public static PolyTime ofNullable( Time value ) {
        return value == null ? PolyTime.of( (Integer) null ) : of( value );
    }


    public static PolyTime of( Integer value ) {
        return new PolyTime( value, ProtoTime.TimeUnit.MILLISECOND );
    }


    public static PolyTime of( Time value ) {
        long time = value.getTime();
        time = time + LOCAL_TZ.getOffset( time );
        return new PolyTime( (int) time, ProtoTime.TimeUnit.MILLISECOND );
    }


    public Time asSqlTime() {
        return new Time( ofDay );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isTime() ) {
            return -1;
        }

        try {
            return Long.compare( ofDay, o.asTime().ofDay );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown." );
        }
    }


    @Override
    public Long getMilliSinceEpoch() {
        return Long.valueOf( ofDay );
    }


    @Override
    public String toString() {
        return ofDay.toString() + timeUnit.toString();
    }

}

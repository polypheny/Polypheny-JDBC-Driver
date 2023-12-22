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

import java.util.Date;
import java.util.TimeZone;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;

public class PolyDate extends PolyTemporal {

    public static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    @Getter
    public Long milliSinceEpoch;


    public PolyDate( long milliSinceEpoch ) {
        super( ProtoPolyType.DATE );
        this.milliSinceEpoch = milliSinceEpoch;
    }


    public static PolyDate of( Number number ) {
        return new PolyDate( number.longValue() );
    }


    public static PolyDate ofNullable( Number number ) {
        return number == null ? null : of( number );
    }


    public static PolyDate ofNullable( java.sql.Date date ) {
        return PolyDate.of( date );
    }


    public Date asDefaultDate() {
        return new Date( milliSinceEpoch );
    }


    public java.sql.Date asSqlDate() {
        return new java.sql.Date( milliSinceEpoch );
    }


    public static PolyDate of( Date date ) {
        long time = date.getTime();
        time = time + LOCAL_TZ.getOffset( time );
        return new PolyDate( time );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isDate() ) {
            return -1;
        }
        try {
            return Long.compare( milliSinceEpoch, o.asDate().milliSinceEpoch );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown!" );
        }
    }


    @Override
    public String toString() {
        return milliSinceEpoch.toString();
    }

}

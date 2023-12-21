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

package org.polypheny.jdbc.nativetypes.category;

import java.util.Calendar;
import java.util.GregorianCalendar;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public abstract class PolyTemporal extends PolyValue {

    private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000;


    public abstract Long getMilliSinceEpoch();


    public PolyTemporal( ProtoValueType type ) {
        super( type );
    }


    public long getDaysSinceEpoch() {
        return getMilliSinceEpoch() / MILLIS_PER_DAY;
    }


    public Calendar toCalendar() {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTimeInMillis( getMilliSinceEpoch() );
        return cal;
    }

}

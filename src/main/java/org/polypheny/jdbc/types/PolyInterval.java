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

import lombok.Getter;

public class PolyInterval {

    @Getter
    private final long months;
    @Getter
    private final long milliseconds;


    public PolyInterval( long months, long milliseconds ) {
        this.months = months;
        this.milliseconds = milliseconds;
    }


    private String plural( long count, String word ) {
        return count + " " + (count != 1 ? word + "s" : word);
    }


    @Override
    public String toString() {
        return plural( months, "month" ) + plural( milliseconds, "milliseconds" );
    }


    @Override
    public boolean equals( Object o ) {
        if ( o instanceof PolyInterval ) {
            PolyInterval i = (PolyInterval) o;
            return months == i.getMonths() && milliseconds == i.getMilliseconds();
        }
        return false;
    }

}

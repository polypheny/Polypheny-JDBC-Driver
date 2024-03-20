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

import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class PolyInterval extends PolyValue {

    public long value;
    public Unit unit;


    public enum Unit {
        MILLISECONDS,
        MONTHS,
    }


    public PolyInterval( long value, Unit unit ) {
        super( ProtoPolyType.UNSPECIFIED ); // TODO: Fix type
        this.value = value;
    }


    public static PolyInterval of( long value, Unit unit ) {
        return new PolyInterval( value, unit );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        return 0;
    }


    @Override
    public String toString() {
        return value + " " + unit.toString().toLowerCase();
    }

}

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

import java.math.BigDecimal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class PolyInterval extends PolyValue {

    public BigDecimal value;


    public PolyInterval( BigDecimal value, ProtoPolyType intervalType ) {
        super( intervalType );
        if ( !TypeUtils.INTERVAL_TYPES.contains( intervalType ) ) {
            throw new RuntimeException( "Type must be an interval type." );
        }
        this.value = value;
    }


    public static PolyInterval of( BigDecimal value, ProtoPolyType intervalType ) {
        return new PolyInterval( value, intervalType );
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
        return value.toString() + type.toString();
    }

}

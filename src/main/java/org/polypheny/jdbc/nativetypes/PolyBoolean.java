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

import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class PolyBoolean extends PolyValue{
    public static final PolyBoolean TRUE = PolyBoolean.of( true );
    public static final PolyBoolean FALSE = PolyBoolean.of( false );

    public Boolean value;

    public PolyBoolean( Boolean value ) {
        super( ProtoValueType.BOOLEAN );
        this.value = value;
    }

    public static PolyBoolean of( Boolean value ) {
        return new PolyBoolean( value );
    }


    public static PolyBoolean ofNullable( Boolean value ) {
        return value == null ? null : of( value );
    }

    public static PolyBoolean of( boolean value ) {
        return new PolyBoolean( value );
    }

    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( isSameType( o ) ) {
            try {
                return ObjectUtils.compare( value, o.asBoolean().value );
            } catch ( ProtoInterfaceServiceException e ) {
                throw new RuntimeException("Should never be thrown!");
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        return value.toString();
    }

}

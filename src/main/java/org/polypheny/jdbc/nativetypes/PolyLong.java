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
import java.math.MathContext;
import java.util.Objects;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;

public class PolyLong extends PolyNumber {

    public Long value;


    public PolyLong( Long value ) {
        super( ProtoPolyType.BIGINT );
        this.value = value;
    }


    public PolyLong( long value ) {
        super( ProtoPolyType.BIGINT );
        this.value = value;
    }


    public static PolyLong of( long value ) {
        return new PolyLong( value );
    }


    public static PolyLong of( Long value ) {
        return new PolyLong( value );
    }


    public static PolyLong of( Number value ) {
        return new PolyLong( value.longValue() );
    }


    public static PolyLong ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    public static PolyLong from( PolyValue value ) throws PrismInterfaceServiceException {
        if ( TypeUtils.NUMERIC_TYPES.contains( value.type ) ) {
            return PolyLong.of( value.asNumber().longValue() );
        }
        throw new PrismInterfaceServiceException(
                PrismInterfaceErrors.DATA_TYPE_MISSMATCH,
                String.format( "%s does not support conversion to %s.", value, value.type )
        );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }
        return ObjectUtils.compare( value, o.asNumber().LongValue() );
    }


    @Override
    public int intValue() {
        return Math.toIntExact( value );
    }


    @Override
    public long longValue() {
        return value;
    }


    @Override
    public float floatValue() {
        return value.floatValue();
    }


    @Override
    public double doubleValue() {
        return value;
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf( value );
    }


    @Override
    public PolyLong increment() {
        return PolyLong.of( value + 1 );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyLong.of( value * other.longValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( other.bigDecimalValue() ) ) : PolyLong.of( value + other.longValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( other.bigDecimalValue() ) ) : PolyLong.of( value - other.longValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyLong.of( -value );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }

        PolyLong polyLong = (PolyLong) o;
        return Objects.equals( value, polyLong.value );
    }


    @Override
    public String toString() {
        return value.toString();
    }

}

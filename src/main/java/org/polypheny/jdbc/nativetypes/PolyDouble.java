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
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class PolyDouble extends PolyNumber {

    public Double value;


    public PolyDouble( Double value ) {
        super( ProtoValueType.DOUBLE );
        this.value = value;
    }


    public static PolyDouble of( Double value ) {
        return new PolyDouble( value );
    }


    public static PolyDouble of( Number value ) {
        return new PolyDouble( value.doubleValue() );
    }


    public static PolyDouble ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }

        try {
            return ObjectUtils.compare( value, o.asNumber().DoubleValue() );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown!" );
        }
    }


    @Override
    public int intValue() {
        return value.intValue();
    }


    @Override
    public long longValue() {
        return value.longValue();
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
    public PolyDouble increment() {
        return PolyDouble.of( value + 1 );
    }


    @Override
    public @NotNull PolyDouble divide( @NotNull PolyNumber other ) {
        return PolyDouble.of( value / other.doubleValue() );
    }


    @Override
    public @NotNull PolyDouble multiply( @NotNull PolyNumber other ) {
        return PolyDouble.of( value * other.doubleValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber b1 ) {
        return PolyDouble.of( value + b1.doubleValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return PolyDouble.of( value - b1.doubleValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyDouble.of( -value );
    }


    @Override
    public String toString() {
        return value.toString();
    }

}
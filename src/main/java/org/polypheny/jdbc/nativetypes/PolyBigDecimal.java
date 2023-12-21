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
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public class PolyBigDecimal extends PolyNumber {

    public BigDecimal value;


    public PolyBigDecimal( BigDecimal value ) {
        super( ProtoValueType.DECIMAL );
        this.value = value;
    }


    public static PolyBigDecimal of( BigDecimal value ) {
        return new PolyBigDecimal( value );
    }


    public static PolyBigDecimal of( String value ) {
        return new PolyBigDecimal( new BigDecimal( value ) );
    }


    public static PolyBigDecimal of( long value ) {
        return new PolyBigDecimal( BigDecimal.valueOf( value ) );
    }


    public static PolyBigDecimal of( double value ) {
        return new PolyBigDecimal( BigDecimal.valueOf( value ) );
    }


    public static PolyBigDecimal ofNullable( Number value ) {
        return value == null ? null : of( value.doubleValue() );
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
        return value.doubleValue();
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return value;
    }


    @Override
    public PolyNumber increment() {
        return PolyBigDecimal.of( value.add( BigDecimal.ONE ) );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( value.divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( value.multiply( other.bigDecimalValue() ) );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber b1 ) {
        return PolyBigDecimal.of( value.add( b1.bigDecimalValue() ) );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return PolyBigDecimal.of( value.subtract( b1.bigDecimalValue() ) );
    }


    @Override
    public PolyBigDecimal negate() {
        return PolyBigDecimal.of( value.negate() );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        PolyBigDecimal that = (PolyBigDecimal) o;
        return Objects.equals( value.stripTrailingZeros(), that.value.stripTrailingZeros() );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }
        try {
            return ObjectUtils.compare( value, o.asNumber().BigDecimalValue() );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown!" );
        }
    }


    @Override
    public String toString() {
        return value.toString();
    }

}

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

import java.math.BigDecimal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.nativetypes.PolyBigDecimal;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.jdbc.nativetypes.TypeUtils;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public abstract class PolyNumber extends PolyValue {
    public PolyNumber( ProtoValueType type ) {
        super( type );
    }


    public static int compareTo( PolyNumber b0, PolyNumber b1 ) {
        if ( b0 == null || b1 == null ) {
            return -1;
        }
        if ( b0.isApprox() || b1.isApprox() ) {
            return b0.DoubleValue().compareTo( b1.DoubleValue() );
        }
        return b0.LongValue().compareTo( b1.LongValue() );
    }


    private boolean isApprox() {
        return TypeUtils.APPROX_TYPES.contains( type );
    }


    @Override
    public boolean equals( Object o ) {
        return super.equals( o );
    }

    public abstract int intValue();


    public Integer IntValue() {
        if ( isNull() ) {
            return null;
        }
        return intValue();
    }


    public abstract long longValue();


    public Long LongValue() {
        if ( isNull() ) {
            return null;
        }
        return longValue();
    }

    public abstract float floatValue();


    public Float FloatValue() {
        if ( isNull() ) {
            return null;
        }
        return floatValue();
    }

    public abstract double doubleValue();


    public Double DoubleValue() {
        if ( isNull() ) {
            return null;
        }
        return doubleValue();
    }

    public abstract BigDecimal bigDecimalValue();


    public BigDecimal BigDecimalValue() {
        if ( isNull() ) {
            return null;
        }
        return bigDecimalValue();
    }


    public abstract PolyNumber increment();

    @NotNull
    public abstract PolyNumber divide( @NotNull PolyNumber other );


    @NotNull
    public abstract PolyNumber multiply( @NotNull PolyNumber other );

    @NotNull
    public abstract PolyNumber plus( @NotNull PolyNumber b1 );

    @NotNull
    public abstract PolyNumber subtract( @NotNull PolyNumber b1 );


    @NotNull
    public PolyNumber floor( @NotNull PolyNumber b1 ) {
        //TODO: optimize this
        final BigDecimal[] bigDecimals = bigDecimalValue().divideAndRemainder( b1.bigDecimalValue() );
        BigDecimal r = bigDecimals[1];
        if ( r.signum() < 0 ) {
            r = r.add( b1.bigDecimalValue() );
        }
        return PolyBigDecimal.of( bigDecimalValue().subtract( r ) );
    }


    public boolean isDecimal() {
        return TypeUtils.FRACTIONAL_TYPES.contains( type );
    }


    public PolyNumber ceil( PolyNumber b1 ) {
        final BigDecimal[] bigDecimals = bigDecimalValue().divideAndRemainder( b1.bigDecimalValue() );
        BigDecimal r = bigDecimals[1];
        if ( r.signum() > 0 ) {
            r = r.subtract( b1.bigDecimalValue() );
        }
        return PolyBigDecimal.of( bigDecimalValue().subtract( r ) );
    }


    public abstract PolyNumber negate();
}

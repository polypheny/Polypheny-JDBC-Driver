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
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public class PolyInteger extends PolyNumber{
    public static final PolyInteger ZERO = PolyInteger.of( 0 );
    public Integer value;

    public PolyInteger( Integer value ) {
        super( ProtoValueType.INTEGER );
        this.value = value;
    }
    public static PolyInteger of( byte value ) {
        return new PolyInteger( (int) value );
    }


    public static PolyInteger of( short value ) {
        return new PolyInteger( (int) value );
    }


    public static PolyInteger of( int value ) {
        return new PolyInteger( value );
    }


    public static PolyInteger of( Integer value ) {
        return new PolyInteger( value );
    }


    public static PolyInteger of( Number value ) {
        if ( value == null ) {
            return null;
        }
        return new PolyInteger( value.intValue() );
    }


    public static PolyInteger ofNullable( Number value ) {
        return value == null ? null : of( value );
    }

    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( !(o instanceof PolyValue) ) {
            return false;
        }
        PolyValue val = (PolyValue) o;

        if ( val.isNumber() ) {
            try {
                return PolyNumber.compareTo( this, val.asNumber() ) == 0;
            } catch ( ProtoInterfaceServiceException e ) {
                throw new RuntimeException( "Should never be thrown!" );
            }
        }

        return false;
    }

    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }

        try {
            return PolyNumber.compareTo( this, o.asNumber() );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown!" );
        }
    }

    @Override
    public boolean isNull() {
        return value == null;
    }


    @Override
    public int intValue() {
        return value;
    }


    public static PolyValue from( PolyValue value ) throws ProtoInterfaceServiceException {
        if ( TypeUtils.NUMERIC_TYPES.contains( value.type ) ) {
            return PolyInteger.of( value.asNumber().intValue() );
        }

        throw new ProtoInterfaceServiceException(
                ProtoInterfaceErrors.DATA_TYPE_MISSMATCH,
                String.format( "%s does not support conversion to %s.", value, value.type )
        );
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
        return value == null ? null : new BigDecimal( value );
    }


    @Override
    public PolyInteger increment() {
        return PolyInteger.of( value + 1 );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyBigDecimal.of( bigDecimalValue().divide( other.bigDecimalValue(), MathContext.DECIMAL64 ) );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().multiply( other.bigDecimalValue() ) ) : PolyInteger.of( value * other.intValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().add( other.bigDecimalValue() ) ) : PolyInteger.of( value + other.intValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber other ) {
        return other.isDecimal() ? PolyBigDecimal.of( bigDecimalValue().subtract( other.bigDecimalValue() ) ) : PolyInteger.of( value - other.intValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyInteger.of( -value );
    }

    @Override
    public String toString() {
        return value.toString();
    }
}

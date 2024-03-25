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
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;

public class PolyFloat extends PolyNumber {

    public Float value;


    public PolyFloat( Float value ) {
        super( ProtoPolyType.FLOAT );
        this.value = value;
    }


    public static PolyFloat of( Float value ) {
        return new PolyFloat( value );
    }


    public static PolyFloat of( Number value ) {
        return new PolyFloat( value.floatValue() );
    }


    public static PolyFloat ofNullable( Number value ) {
        return value == null ? null : of( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !o.isNumber() ) {
            return -1;
        }
        return ObjectUtils.compare( value, o.asFloat().value );
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
        return value;
    }


    @Override
    public double doubleValue() {
        return value.doubleValue();
    }


    @Override
    public BigDecimal bigDecimalValue() {
        return BigDecimal.valueOf( value );
    }


    @Override
    public PolyNumber increment() {
        return PolyFloat.of( value + 1 );
    }


    @Override
    public @NotNull PolyNumber divide( @NotNull PolyNumber other ) {
        return PolyFloat.of( value / other.floatValue() );
    }


    @Override
    public @NotNull PolyNumber multiply( @NotNull PolyNumber other ) {
        return PolyFloat.of( value * other.floatValue() );
    }


    @Override
    public @NotNull PolyNumber plus( @NotNull PolyNumber b1 ) {
        return PolyFloat.of( value + b1.floatValue() );
    }


    @Override
    public @NotNull PolyNumber subtract( @NotNull PolyNumber b1 ) {
        return PolyFloat.of( value - b1.floatValue() );
    }


    @Override
    public PolyNumber negate() {
        return PolyFloat.of( -value );
    }


    @Override
    public String toString() {
        return value.toString();
    }

}

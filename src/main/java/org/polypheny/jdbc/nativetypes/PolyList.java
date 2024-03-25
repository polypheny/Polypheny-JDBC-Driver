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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "copyOf")
public class PolyList<E extends PolyValue> extends PolyValue implements List<E> {

    @Delegate
    public List<E> value;


    public PolyList( List<E> value ) {
        super( ProtoPolyType.ARRAY );
        this.value = new ArrayList<>( value );
    }


    @SafeVarargs
    public PolyList( E... value ) {
        this( Arrays.asList( value ) );
    }


    public static <E extends PolyValue> PolyList<E> of( Collection<E> value ) {
        return new PolyList<>( new ArrayList<>( value ) );
    }


    @SuppressWarnings("unused")
    public static <E extends PolyValue> PolyList<E> ofNullable( Collection<E> value ) {
        return value == null ? null : new PolyList<>( new ArrayList<>( value ) );
    }


    @SafeVarargs
    public static <E extends PolyValue> PolyList<E> of( E... values ) {
        return new PolyList<>( values );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        PolyList<?> other = o.asList();
        if ( value.size() != other.value.size() ) {
            return value.size() - o.asList().value.size();
        }
        int size = Math.min( value.size(), other.size() );
        for ( int i = 0; i < size; i++ ) {
            if ( value.get( i ).compareTo( other.value.get( i ) ) != 0 ) {
                return value.get( i ).compareTo( other.value.get( i ) );
            }
        }
        return 0;
    }


    @Override
    public String toString() {
        return value.toString();
    }

}

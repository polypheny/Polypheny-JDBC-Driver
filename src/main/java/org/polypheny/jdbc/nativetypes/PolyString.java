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

import com.google.common.base.Charsets;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ObjectUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.protointerface.proto.ProtoPolyType;

public class PolyString extends PolyValue {

    public String value;
    public Charset charset;


    public PolyString( String value ) {
        this( value, Charsets.UTF_16 );
    }


    public PolyString( String value, Charset charset ) {
        super( ProtoPolyType.VARCHAR );
        this.value = value;
        this.charset = charset;
    }


    public static PolyString of( String value, @Nullable String charset ) {
        return new PolyString( value, charset == null ? Charsets.UTF_16 : Charset.forName( charset ) );
    }


    public static PolyString of( String value ) {
        return new PolyString( value );
    }


    public static PolyString ofNullable( String value ) {
        return of( value );
    }


    public static PolyString concat( List<PolyString> strings ) {
        return PolyString.of( strings.stream().map( s -> s.value ).collect( Collectors.joining() ) );
    }


    public static PolyString join( String delimiter, List<PolyString> strings ) {
        return PolyString.of( strings.stream().map( s -> s.value ).collect( Collectors.joining( delimiter ) ) );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }

        return ObjectUtils.compare( value, o.asString().value );
    }


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        PolyString that = (PolyString) o;
        return Objects.equals( value, that.value );
    }


    @Override
    public boolean isNull() {
        return value == null;
    }


    public String asCharset( String charset ) {
        return asCharset( Charset.forName( charset ) );
    }


    public String asCharset( Charset charset ) {
        if ( this.charset.equals( charset ) ) {
            return value;
        }
        return new String( value.getBytes( this.charset ), charset );
    }


    @Override
    public String toString() {
        return value;
    }

}

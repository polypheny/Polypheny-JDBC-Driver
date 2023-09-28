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

import java.util.Base64;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public class PolyBinary extends PolyValue{

    public static final PolyBinary EMPTY = new PolyBinary( new byte[0] );
    public byte[] value;


    public PolyBinary( byte[] value ) {
        super( ProtoValueType.BINARY );
        this.value = value;
    }

    public PolyBinary( byte[] value, ProtoValueType type ) {
        super( type  );
        if (!TypeUtils.BLOB_TYPES.contains( type )) {
            throw new RuntimeException("Should never be thrown.");
        }
        this.value = value;
    }

    public static PolyBinary of( byte[] value ) {
        return new PolyBinary( value);
    }


    public static PolyBinary ofNullable( byte[] value ) {
        return value == null ? null : PolyBinary.of( value );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return 0;
    }

    @Override
    public String toString() {
        return Base64.getEncoder().encodeToString( value );
    }


    public int getBitCount() {
        return value.length;
    }

    public String toHexString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (byte singleByte : value) {
            stringBuilder.append( String.format( "%02x", singleByte) );
        }
        return  stringBuilder.toString();
    }
}

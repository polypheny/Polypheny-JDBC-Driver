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

package org.polypheny.jdbc.nativetypes.relational;

import java.util.HashMap;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.Delegate;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

@EqualsAndHashCode(callSuper = true)
@Value(staticConstructor = "of")
public class PolyMap<K extends PolyValue, V extends PolyValue> extends PolyValue implements Map<K, V> {

    @Delegate
    public Map<K, V> map;


    public PolyMap( Map<K, V> map ) {
        this( map, ProtoValueType.MAP );
    }


    public PolyMap( Map<K, V> map, ProtoValueType type ) {
        super( type );
        this.map = new HashMap<>( map );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isSameType( o ) ) {
            return -1;
        }
        Map<PolyValue, PolyValue> other = null;
        try {
            other = o.asMap();
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown." );
        }
        if ( map.size() != other.size() ) {

            return map.size() > other.size() ? 1 : -1;
        }

        for ( Entry<PolyValue, PolyValue> entry : other.entrySet() ) {
            if ( other.containsKey( entry.getKey() ) ) {
                int i = entry.getValue().compareTo( other.get( entry.getKey() ) );
                if ( i != 0 ) {
                    return i;
                }
            } else {
                return -1;
            }
        }
        return 0;
    }

}

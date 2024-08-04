/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.jdbc.utils;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PrismInterfaceClient;
import org.polypheny.jdbc.types.TypedValue;
import org.polypheny.prism.ProtoString;
import org.polypheny.prism.ProtoValue;

public class ProtoUtils {

    public static ProtoValue serializeAsProtoString( String string ) {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( string )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .build();
    }


    public static List<ProtoValue> serializeParameterList( List<TypedValue> values, PrismInterfaceClient client ) {
        return values.stream().map( v -> {
            try {
                return v.serialize(client);
            } catch ( SQLException e ) {
                throw new RuntimeException( "Should not be thrown. Encountered an unknown type during serialization." );
            }
        } ).collect( Collectors.toList() );
    }


    public static Map<String, ProtoValue> serializeParameterMap(Map<String, TypedValue> values, PrismInterfaceClient client) {
        return values.entrySet().stream().collect(Collectors.toMap(
                Entry::getKey,
                value -> {
                    try {
                        return value.getValue().serialize(client);
                    } catch (Exception e) {
                        throw new RuntimeException("Serialization failed", e);
                    }
                }
        ));
    }

}

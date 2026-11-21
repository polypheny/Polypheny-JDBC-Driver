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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.jdbc.PrismInterfaceErrors;
import org.polypheny.jdbc.PrismInterfaceServiceException;
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


    public static List<ProtoValue> serializeParameterList( List<TypedValue> values ) throws PrismInterfaceServiceException {
        List<ProtoValue> parameters = new ArrayList<>( values.size() );
        for ( TypedValue value : values ) {
            try {
                parameters.add( value.serialize() );
            } catch ( SQLException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Could not serialize parameter " + (parameters.size() + 1) + ".", e );
            }
        }
        return parameters;
    }


    public static Map<String, ProtoValue> serializeParameterMap( Map<String, TypedValue> values ) throws PrismInterfaceServiceException {
        Map<String, ProtoValue> parameters = new HashMap<>();
        for ( Entry<String, TypedValue> entry : values.entrySet() ) {
            try {
                TypedValue value = entry.getValue() == null ? TypedValue.fromNull() : entry.getValue();
                parameters.put( entry.getKey(), value.serialize() );
            } catch ( SQLException e ) {
                throw new PrismInterfaceServiceException( PrismInterfaceErrors.DATA_TYPE_MISMATCH, "Could not serialize parameter " + entry.getKey() + ".", e );
            }
        }
        return parameters;
    }

}

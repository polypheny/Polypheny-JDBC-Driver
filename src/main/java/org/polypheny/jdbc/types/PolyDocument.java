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

package org.polypheny.jdbc.types;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.stream.Collectors;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoValue;

public class PolyDocument extends HashMap<String, TypedValue> {

    public PolyDocument() {
        super();
    }


    public PolyDocument( HashMap<String, TypedValue> entries ) {
        super( entries );
    }


    public PolyDocument( ProtoDocument document ) {
        super();
        document.getEntriesMap()
                .forEach( ( k, v ) -> put(
                        k,
                        new TypedValue( v )
                ) );
    }


    private ProtoValue serializeValue( TypedValue value ) {
        try {
            return value.serialize();
        } catch ( SQLException e ) {
            throw new RuntimeException( "Cannot serialize value: ", e );
        }
    }


    public ProtoDocument serialize() {
        return ProtoDocument.newBuilder().putAllEntries( entrySet().stream().collect(
                Collectors.toMap( Entry::getKey, e -> serializeValue( e.getValue() ) ) ) ).build();
    }

}

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
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.jdbc.streaming.StreamingIndex;
import org.polypheny.jdbc.utils.ProtoUtils;
import org.polypheny.prism.ProtoDocument;
import org.polypheny.prism.ProtoEntry;
import org.polypheny.prism.ProtoValue;
import org.polypheny.prism.ProtoValue.ValueCase;

public class PolyDocument extends HashMap<String, TypedValue> {

    public PolyDocument() {
        super();
    }


    public PolyDocument( ProtoDocument document, PolyConnection polyConnection ) {
        super();
        document.getEntriesList().stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .forEach( e -> put(
                        e.getKey().getString().getString(),
                        new TypedValue( e.getValue(), polyConnection )
                ) );
    }


    public ProtoDocument serialize( StreamingIndex streamingIndex ) {
        List<ProtoEntry> protoEntries = entrySet().stream().map( entry -> {
            ProtoValue protoKey = ProtoUtils.serializeAsProtoString( entry.getKey() );
            ProtoValue protoValue;
            try {
                protoValue = entry.getValue().serialize( streamingIndex );
            } catch ( SQLException e ) {
                throw new RuntimeException( "Should not be thrown. Unknown value encountered." );
            }
            return ProtoEntry.newBuilder()
                    .setKey( protoKey )
                    .setValue( protoValue )
                    .build();
        } ).collect( Collectors.toList() );

        return ProtoDocument.newBuilder().addAllEntries( protoEntries ).build();
    }

}

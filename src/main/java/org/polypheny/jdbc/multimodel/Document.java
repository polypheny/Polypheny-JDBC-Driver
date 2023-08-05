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

package org.polypheny.jdbc.multimodel;

import java.util.LinkedHashMap;
import java.util.Set;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.ProtoDocument;
import org.polypheny.jdbc.proto.ProtoEntry;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class Document {

    public Document( ProtoDocument protoDocument ) throws ProtoInterfaceServiceException {
        entries = new LinkedHashMap<>();
        for ( ProtoEntry protoEntry : protoDocument.getEntriesList() ) {
            if ( protoEntry.getKey().getType() != ProtoValueType.VARCHAR ) {
                throw new ProtoInterfaceServiceException(
                        ProtoInterfaceErrors.DATA_TYPE_MISSMATCH,
                        "Keys for document entries mst be strings."
                );
            }
            entries.put( protoEntry.getKey().getString().getString(), protoEntry.getValue() );
        }
    }


    private LinkedHashMap<String, ProtoValue> entries;


    Set<String> getEntryKeys() {
        return entries.keySet();
    }


    ProtoValue.ProtoValueType getEntryType( String key ) throws ProtoInterfaceServiceException {
        ProtoValue protoValue = entries.get( key );
        if ( protoValue == null ) {
            throw new ProtoInterfaceServiceException(
                    ProtoInterfaceErrors.ENTRY_NOT_EXISTS,
                    "There exists no entry with key " + key
            );
        }
        return protoValue.getType();
    }


    ProtoValue getEntry( String key ) {
        return entries.get( key );
    }

}

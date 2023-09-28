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

package org.polypheny.jdbc.nativetypes.graph;
import java.util.List;
import java.util.Map.Entry;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.jdbc.nativetypes.PolyList;
import org.polypheny.jdbc.nativetypes.PolyString;
import org.polypheny.db.protointerface.proto.ProtoValue.ProtoValueType;

public abstract class GraphPropertyHolder extends GraphObject {
    public final PolyDictionary properties;
    public final PolyList<PolyString> labels;

    public GraphPropertyHolder( PolyString id, ProtoValueType type, PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        super( id, type, variableName );
        this.properties = properties;
        this.labels = PolyList.of( labels );
    }

    public boolean matchesProperties( PolyDictionary properties ) {
        for ( Entry<PolyString, PolyValue> entry : properties.entrySet() ) {
            if ( !this.properties.containsKey( entry.getKey() ) ) {
                return false;
            }
            if ( !this.properties.get( entry.getKey() ).toString().equals( entry.getValue().toString() ) ) {
                return false;
            }
        }
        return true;
    }


    public boolean matchesLabels( PolyList<PolyString> labels ) {
        return this.labels.equals( labels );
    }


    public boolean labelAndPropertyMatch( GraphPropertyHolder other ) {
        // we don't need to match the source and target, this is done via segments or paths
        if ( !other.properties.isEmpty() ) {
            if ( !matchesProperties( other.properties ) ) {
                return false;
            }
        }
        if ( !other.labels.isEmpty() ) {
            return matchesLabels( other.labels );
        }
        return true;
    }


    public abstract void setLabels( PolyList<PolyString> labels );
}

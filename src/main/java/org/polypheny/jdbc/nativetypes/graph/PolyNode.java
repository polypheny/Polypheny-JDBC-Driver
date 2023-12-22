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
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.PolyList;
import org.polypheny.jdbc.nativetypes.PolyString;
import org.polypheny.jdbc.nativetypes.PolyValue;

public class PolyNode extends GraphPropertyHolder {

    @Getter
    @Setter
    @Accessors(fluent = true)
    private boolean isVariable = false;


    public PolyNode( @NonNull PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, variableName );
    }


    public PolyNode( PolyString id, @NonNull PolyDictionary properties, List<PolyString> labels, PolyString variableName ) {
        super( id, ProtoPolyType.NODE, properties, labels, variableName );
    }


    @Override
    public String toString() {
        return "PolyNode{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                '}';
    }


    public boolean isBlank() {
        // MATCH (n) -> true, MATCH (n{name: 'Max'}) -> false, MATCH (n:Person) -> false
        return (properties == null || properties.isEmpty()) && (labels == null || labels.isEmpty());
    }


    @Override
    public void setLabels( PolyList<PolyString> labels ) {
        this.labels.addAll( labels );
    }


    public PolyNode copyNamed( PolyString variableName ) {
        if ( variableName == null ) {
            // no copy needed
            return this;
        }
        return new PolyNode( id, properties, labels, variableName );

    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isNode() ) {
            return -1;
        }

        try {
            return id.compareTo( o.asNode().id );
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown." );
        }
    }

}

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

import lombok.Getter;
import org.polypheny.prism.ProtoEdge;

@Getter
public class PolyEdge extends PolyGraphElement {

    private final String left;
    private final String right;
    private final EdgeDirection direction;


    public PolyEdge( ProtoEdge protoEdge ) {
        super();
        this.id = protoEdge.getId();
        this.name = protoEdge.getName();
        this.labels = protoEdge.getLabelsList();
        protoEdge.getPropertiesMap().forEach( ( k, v ) -> put( k, new TypedValue( v ) ) );
        this.left = protoEdge.getSource();
        this.right = protoEdge.getTarget();
        this.direction = EdgeDirection.valueOf( protoEdge.getDirection().name() );
    }


    enum EdgeDirection {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        NONE
    }

}

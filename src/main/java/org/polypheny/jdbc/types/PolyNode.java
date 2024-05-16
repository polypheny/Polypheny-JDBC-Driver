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

import org.polypheny.prism.ProtoNode;
import org.polypheny.prism.ProtoValue.ValueCase;

public class PolyNode extends PolyGraphElement {


    public PolyNode( ProtoNode protoNode ) {
        super();
        this.id = protoNode.getId();
        this.name = protoNode.getName();
        this.labels = protoNode.getLabelsList();
        protoNode.getPropertiesList().stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .forEach( p -> put(
                        p.getKey().getString().getString(),
                        new TypedValue( p.getValue() )
                ) );
    }

}

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

import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyConnection;
import org.polypheny.prism.ProtoNode;

public class PolyNode extends PolyGraphElement {


    public PolyNode( ProtoNode protoNode, PolyConnection polyConnection ) {
        super( ElementType.NODE );
        this.id = protoNode.getId();
        this.name = protoNode.getName();
        this.labels = protoNode.getLabelsList();
        putAll( protoNode.getPropertiesMap().entrySet().stream().collect( Collectors.toMap(
                Entry::getKey, // keys are always strings
                e -> new TypedValue( e.getValue(), polyConnection )
        ) ) );
    }

}

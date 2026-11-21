/*
 * Copyright 2019-2026 The Polypheny Project
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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.prism.ProtoPath;

/**
 * A path in traversal order. Edge i connects nodes i and i+1 and keeps its original orientation.
 */
@Getter
public final class PolyPath {

    private final List<PolyNode> nodes;
    private final List<PolyEdge> edges;


    public PolyPath( ProtoPath path ) {
        this.nodes = Collections.unmodifiableList( path.getNodesList().stream().map( PolyNode::new ).collect( Collectors.toList() ) );
        this.edges = Collections.unmodifiableList( path.getEdgesList().stream().map( PolyEdge::new ).collect( Collectors.toList() ) );
    }

}

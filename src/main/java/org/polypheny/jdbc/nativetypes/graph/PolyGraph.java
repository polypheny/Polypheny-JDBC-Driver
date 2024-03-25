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

import java.util.Map;
import java.util.UUID;
import lombok.NonNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.nativetypes.PolyString;
import org.polypheny.jdbc.nativetypes.PolyValue;

public class PolyGraph extends GraphObject {

    private final Map<PolyString, PolyNode> nodes;
    private final Map<PolyString, PolyEdge> edges;


    public PolyGraph( @NonNull Map<PolyString, PolyNode> nodes, @NonNull Map<PolyString, PolyEdge> edges ) {
        this( PolyString.of( UUID.randomUUID().toString() ), nodes, edges );
    }


    public PolyGraph( PolyString id, @NonNull Map<PolyString, PolyNode> nodes, @NonNull Map<PolyString, PolyEdge> edges ) {
        super( id, ProtoPolyType.GRAPH, null );
        this.nodes = nodes;
        this.edges = edges;
    }


    @Override
    public int compareTo( PolyValue other ) {
        if ( !other.isGraph() ) {
            return -1;
        }
        PolyGraph o = null;
        o = other.asGraph();

        if ( this.nodes.size() > o.nodes.size() ) {
            return 1;
        }
        if ( this.nodes.size() < o.nodes.size() ) {
            return -1;
        }

        if ( this.nodes.keySet().equals( o.nodes.keySet() ) && this.edges.values().equals( o.edges.values() ) ) {
            return 0;
        }
        return -1;
    }


    @Override
    public String toString() {
        return "PolyGraph{" +
                "nodes=" + nodes +
                ", edges=" + edges +
                '}';
    }

}

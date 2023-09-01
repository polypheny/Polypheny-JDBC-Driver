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
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.experimental.NonFinal;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.PolyList;
import org.polypheny.jdbc.nativetypes.PolyString;
import org.polypheny.jdbc.nativetypes.PolyValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class PolyEdge extends GraphPropertyHolder {
    public PolyString source;
    public PolyString target;
    public EdgeDirection direction;

    @Setter
    @NonFinal
    @Accessors(fluent = true)
    Integer from;
    Integer to;


    public PolyEdge( @NonNull PolyDictionary properties, List<PolyString> labels, PolyString source, PolyString target, EdgeDirection direction, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, source, target, direction, variableName );
    }


    public PolyEdge( PolyString id, @NonNull PolyDictionary properties, List<PolyString> labels, PolyString source, PolyString target, EdgeDirection direction, PolyString variableName ) {
        super( id, ProtoValueType.EDGE, properties, labels, variableName );
        this.source = source;
        this.target = target;
        this.direction = direction;
    }


    public int getVariants() {
        if ( from == null || to == null ) {
            return 1;
        }
        return to - from + 1;
    }


    public PolyEdge from( PolyString left, PolyString right ) {
        return new PolyEdge( id, properties, labels, left == null ? this.source : left, right == null ? this.target : right, direction, null );
    }


    @Override
    public void setLabels( PolyList<PolyString> labels ) {
        this.labels.clear();
        this.labels.add( labels.get( 0 ) );
    }

    public boolean isRange() {
        if ( from == null || to == null ) {
            return false;
        }
        return !from.equals(to);
    }

    public int getMinLength() {
        if ( from == null  ) {
            return 1;
        }
        return from;
    }


    public String getRangeDescriptor() {
        if ( from == null || to == null ) {
            return "";
        }
        String range = "*";

        if ( from.equals( to ) ) {
            return range + to;
        }
        return range + from + ".." + to;
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !this.isSameType( o ) ) {
            return -1;
        }
        try {
            return this.equals( o.asEdge() ) ? 0 : -1;
        } catch ( ProtoInterfaceServiceException e ) {
            throw new RuntimeException( "Should never be thrown." );
        }

    }

    public enum EdgeDirection {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        NONE
    }


    @Override
    public String toString() {
        return "PolyEdge{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                ", leftId=" + source +
                ", rightId=" + target +
                ", direction=" + direction +
                '}';
    }

}

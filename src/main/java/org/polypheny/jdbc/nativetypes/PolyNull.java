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

package org.polypheny.jdbc.nativetypes;

import java.math.BigDecimal;
import java.util.HashMap;
import lombok.NonNull;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.nativetypes.category.PolyBlob;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.nativetypes.graph.PolyDictionary;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge;
import org.polypheny.jdbc.nativetypes.graph.PolyGraph;
import org.polypheny.jdbc.nativetypes.graph.PolyNode;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public class PolyNull extends PolyValue {

    public static PolyNull NULL = new PolyNull();


    public PolyNull() {
        super( ProtoValueType.NULL );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return o.isNull() ? 0 : -1;
    }


    @Override
    public boolean isBoolean() {
        return true;
    }


    @Override
    public @NotNull PolyBoolean asBoolean() {
        return PolyBoolean.of( null );
    }


    @Override
    public boolean isInteger() {
        return true;
    }


    @Override
    public @NotNull PolyInteger asInteger() {
        return PolyInteger.of( null );
    }


    @Override
    public boolean isDocument() {
        return true;
    }


    @Override
    public @NotNull PolyDocument asDocument() {
        return PolyDocument.ofDocument( null );
    }


    @Override
    public boolean isList() {
        return true;
    }


    @Override
    public @NotNull <T extends PolyValue> PolyList<T> asList() {
        return PolyList.of();
    }


    @Override
    public boolean isString() {
        return true;
    }


    @Override
    public @NotNull PolyString asString() {
        return PolyString.of( null );
    }


    @Override
    public boolean isBinary() {
        return true;
    }


    @Override
    public @NotNull PolyBinary asBinary() {
        return PolyBinary.of( null );
    }


    @Override
    public boolean isBigDecimal() {
        return true;
    }


    @Override
    public @NotNull PolyBigDecimal asBigDecimal() {
        return PolyBigDecimal.of( (BigDecimal) null );
    }


    @Override
    public boolean isFloat() {
        return true;
    }


    @Override
    public @NotNull PolyFloat asFloat() {
        return PolyFloat.of( null );
    }


    @Override
    public boolean isDouble() {
        return true;
    }


    @Override
    public @NotNull PolyDouble asDouble() {
        return PolyDouble.of( null );
    }


    @Override
    public boolean isLong() {
        return true;
    }


    @Override
    public @NotNull PolyLong asLong() {
        return PolyLong.of( (Long) null );
    }


    @Override
    public boolean isTemporal() {
        return true;
    }


    @Override
    public PolyTemporal asTemporal() {
        return PolyDate.of( (Long) null );
    }


    @Override
    public boolean isDate() {
        return true;
    }


    @Override
    public @NonNull PolyDate asDate() {
        return PolyDate.of( (Long) null );
    }


    @Override
    public boolean isTime() {
        return true;
    }


    @Override
    public @NonNull PolyTime asTime() {
        return (PolyTime) PolyTime.of( (Long) null );
    }


    @Override
    public boolean isTimestamp() {
        return true;
    }


    @Override
    public @NonNull PolyTimeStamp asTimeStamp() {
        return PolyTimeStamp.of( (Long) null );
    }


    @Override
    public boolean isMap() {
        return true;
    }


    @Override
    public @NonNull PolyMap<PolyValue, PolyValue> asMap() {
        return PolyMap.of( null );
    }


    @Override
    public boolean isEdge() {
        return true;
    }


    @Override
    public @NonNull PolyEdge asEdge() {
        return new PolyEdge( new PolyDictionary(), null, null, null, null, null );
    }


    @Override
    public boolean isNode() {
        return true;
    }


    @Override
    public @NonNull PolyNode asNode() {
        return new PolyNode( new PolyDictionary(), null, null );
    }


    @Override
    public boolean isPath() {
        return true;
    }


    @Override
    public boolean isGraph() {
        return true;
    }


    @Override
    public @NonNull PolyGraph asGraph() {
        return new PolyGraph( null, new HashMap<>(), new HashMap<>() );
    }


    @Override
    public boolean isNumber() {
        return true;
    }


    @Override
    public PolyNumber asNumber() {
        return PolyInteger.of( null );
    }


    @Override
    public boolean isInterval() {
        return true;
    }


    @Override
    public PolyInterval asInterval() {
        return PolyInterval.of( null, null );
    }


    @Override
    public boolean isSymbol() {
        return true;
    }


    @Override
    public PolySymbol asSymbol() {
        return PolySymbol.of( null );
    }


    @Override
    public boolean isBlob() {
        return true;
    }


    @Override
    public @NonNull PolyBlob asBlob() {
        return new PolyBlob( ProtoValueType.FILE );
    }


    @Override
    public String toString() {
        return "null";
    }

}

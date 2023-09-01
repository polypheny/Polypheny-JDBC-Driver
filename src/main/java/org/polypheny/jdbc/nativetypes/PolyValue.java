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

import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyBlob;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge;
import org.polypheny.jdbc.nativetypes.graph.PolyGraph;
import org.polypheny.jdbc.nativetypes.graph.PolyNode;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;

public abstract class PolyValue implements Comparable<PolyValue> {
    public ProtoValue.ProtoValueType type;


    public PolyValue( ProtoValueType type ) {
        this.type = type;
    }

    public boolean isSameType( PolyValue value ) {
        return type == value.type;
    }

    public boolean isNull() {
        return type == ProtoValue.ProtoValueType.NULL;
    }


    public PolyNull asNull() {
        return (PolyNull) this;
    }


    public boolean isBoolean() {
        return type == ProtoValue.ProtoValueType.BOOLEAN;
    }


    @NotNull
    public PolyBoolean asBoolean() throws ProtoInterfaceServiceException {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        throw cannotParse( this, PolyBoolean.class );
    }


    @NotNull
    private ProtoInterfaceServiceException cannotParse( PolyValue value, Class<?> clazz ) {
        return new ProtoInterfaceServiceException(
                ProtoInterfaceErrors.WRAPPER_INCORRECT_TYPE,
                String.format( "Cannot parse %s to type %s", value, clazz.getSimpleName() )
                );
    }


    public boolean isInteger() {
        return type == ProtoValue.ProtoValueType.INTEGER;
    }


    @NotNull
    public PolyInteger asInteger() throws ProtoInterfaceServiceException {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }

        throw cannotParse( this, PolyInteger.class );
    }


    public boolean isDocument() {
        return type == ProtoValue.ProtoValueType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() throws ProtoInterfaceServiceException {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw cannotParse( this, PolyDocument.class );
    }


    public boolean isList() {
        return type == ProtoValue.ProtoValueType.ARRAY;
    }


    @NotNull
    public <T extends PolyValue> PolyList<T> asList() throws ProtoInterfaceServiceException {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw cannotParse( this, PolyList.class );
    }


    public boolean isString() {
        return type == ProtoValue.ProtoValueType.VARCHAR;
    }


    @NotNull
    public PolyString asString() throws ProtoInterfaceServiceException {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw cannotParse( this, PolyString.class );
    }


    public boolean isBinary() {
        return type == ProtoValue.ProtoValueType.BINARY;
    }


    @NotNull
    public PolyBinary asBinary() throws ProtoInterfaceServiceException {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw cannotParse( this, PolyBinary.class );
    }


    public boolean isBigDecimal() {
        return type == ProtoValue.ProtoValueType.DECIMAL;
    }


    @NotNull
    public PolyBigDecimal asBigDecimal() throws ProtoInterfaceServiceException {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }

        throw cannotParse( this, PolyBigDecimal.class );
    }


    public boolean isFloat() {
        return type == ProtoValue.ProtoValueType.FLOAT;
    }


    @NotNull
    public PolyFloat asFloat() throws ProtoInterfaceServiceException {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }

        throw cannotParse( this, PolyFloat.class );
    }


    public boolean isDouble() {
        return type == ProtoValue.ProtoValueType.DOUBLE;
    }


    @NotNull
    public PolyDouble asDouble() throws ProtoInterfaceServiceException {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }

        throw cannotParse( this, PolyDouble.class );
    }


    public boolean isLong() {
        return type == ProtoValue.ProtoValueType.BIGINT;
    }


    @NotNull
    public PolyLong asLong() throws ProtoInterfaceServiceException {
        if ( isLong() ) {
            return (PolyLong) this;
        }

        throw cannotParse( this, PolyLong.class );
    }


    public boolean isTemporal() {
        return TypeUtils.DATETIME_TYPES.contains( type );
    }


    public PolyTemporal asTemporal() throws ProtoInterfaceServiceException {
        if ( isTemporal() ) {
            return (PolyTemporal) this;
        }
        throw cannotParse( this, PolyTemporal.class );
    }


    public boolean isDate() {
        return type == ProtoValue.ProtoValueType.DATE;
    }


    @NotNull
    public PolyDate asDate() throws ProtoInterfaceServiceException {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw cannotParse( this, PolyDate.class );
    }


    public boolean isTime() {
        return type == ProtoValue.ProtoValueType.TIME;
    }


    @NotNull
    public PolyTime asTime() throws ProtoInterfaceServiceException {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw cannotParse( this, PolyTime.class );
    }


    public boolean isTimestamp() {
        return type == ProtoValue.ProtoValueType.TIMESTAMP;
    }


    @NotNull
    public PolyTimeStamp asTimeStamp() throws ProtoInterfaceServiceException {
        if ( isTimestamp() ) {
            return (PolyTimeStamp) this;
        }

        throw cannotParse( this, PolyTimeStamp.class );
    }


    public boolean isMap() {
        return type == ProtoValue.ProtoValueType.MAP;
    }


    @NotNull
    public PolyMap<PolyValue, PolyValue> asMap() throws ProtoInterfaceServiceException {
        if ( isMap() || isDocument() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw cannotParse( this, PolyMap.class );
    }


    public boolean isEdge() {
        return type == ProtoValue.ProtoValueType.EDGE;
    }


    @NotNull
    public PolyEdge asEdge() throws ProtoInterfaceServiceException {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw cannotParse( this, PolyEdge.class );
    }


    public boolean isNode() {
        return type == ProtoValue.ProtoValueType.NODE;
    }


    @NotNull
    public PolyNode asNode() throws ProtoInterfaceServiceException {
        if ( isNode() ) {
            return (PolyNode) this;
        }
        throw cannotParse( this, PolyNode.class );
    }


    public boolean isPath() {
        return type == ProtoValue.ProtoValueType.PATH;
    }


    public boolean isGraph() {
        return type == ProtoValue.ProtoValueType.GRAPH;
    }


    @NotNull
    public PolyGraph asGraph() throws ProtoInterfaceServiceException {
        if ( isGraph() ) {
            return (PolyGraph) this;
        }
        throw cannotParse( this, PolyGraph.class );
    }


    public boolean isNumber() {
        return TypeUtils.NUMERIC_TYPES.contains( type );
    }


    public PolyNumber asNumber() throws ProtoInterfaceServiceException {
        if ( isNumber() ) {
            return (PolyNumber) this;
        }
        throw cannotParse( this, PolyNumber.class );
    }


    public boolean isInterval() {
        return TypeUtils.INTERVAL_TYPES.contains( type );
    }


    public PolyInterval asInterval() throws ProtoInterfaceServiceException {
        if ( isInterval() ) {
            return (PolyInterval) this;
        }
        throw cannotParse( this, PolyInterval.class );
    }


    public boolean isSymbol() {
        return type == ProtoValue.ProtoValueType.SYMBOL;
    }


    public PolySymbol asSymbol() throws ProtoInterfaceServiceException {
        if ( isSymbol() ) {
            return (PolySymbol) this;
        }
        throw cannotParse( this, PolySymbol.class );
    }


    public boolean isBlob() {
        return TypeUtils.BLOB_TYPES.contains( type );
    }


    @NotNull
    public PolyBlob asBlob() throws ProtoInterfaceServiceException {
        if ( isBlob() ) {
            return (PolyBlob) this;
        }
        throw cannotParse( this, PolyBlob.class );
    }


    public boolean isUserDefinedValue() {
        return ProtoValue.ProtoValueType.USER_DEFINED_TYPE == type;
    }


    public PolyUserDefinedValue asUserDefinedValue() throws ProtoInterfaceServiceException {
        if ( isUserDefinedValue() ) {
            return (PolyUserDefinedValue) this;
        }
        throw cannotParse( this, PolyUserDefinedValue.class );
    }

}

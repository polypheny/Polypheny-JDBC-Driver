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
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.PolyInterval.Unit;
import org.polypheny.jdbc.nativetypes.category.PolyBlob;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge;
import org.polypheny.jdbc.nativetypes.graph.PolyGraph;
import org.polypheny.jdbc.nativetypes.graph.PolyNode;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoDocument;
import org.polypheny.db.protointerface.proto.ProtoEntry;
import org.polypheny.db.protointerface.proto.ProtoList;
import org.polypheny.db.protointerface.proto.ProtoMap;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ValueCase;

public abstract class PolyValue implements Comparable<PolyValue> {

    public ProtoPolyType type;


    public PolyValue( ProtoPolyType type ) {
        this.type = type;
    }


    public boolean isSameType( PolyValue value ) {
        return type == value.type;
    }


    public boolean isNull() {
        return type == ProtoPolyType.NULL;
    }


    public PolyNull asNull() {
        return (PolyNull) this;
    }


    public boolean isBoolean() {
        return type == ProtoPolyType.BOOLEAN;
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
        return type == ProtoPolyType.INTEGER;
    }


    @NotNull
    public PolyInteger asInteger() throws ProtoInterfaceServiceException {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }

        throw cannotParse( this, PolyInteger.class );
    }


    public boolean isDocument() {
        return type == ProtoPolyType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() throws ProtoInterfaceServiceException {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw cannotParse( this, PolyDocument.class );
    }


    public boolean isList() {
        return type == ProtoPolyType.ARRAY;
    }


    @NotNull
    public <T extends PolyValue> PolyList<T> asList() throws ProtoInterfaceServiceException {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw cannotParse( this, PolyList.class );
    }


    public boolean isString() {
        return type == ProtoPolyType.VARCHAR;
    }


    @NotNull
    public PolyString asString() throws ProtoInterfaceServiceException {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw cannotParse( this, PolyString.class );
    }


    public boolean isBinary() {
        return type == ProtoPolyType.BINARY;
    }


    @NotNull
    public PolyBinary asBinary() throws ProtoInterfaceServiceException {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw cannotParse( this, PolyBinary.class );
    }


    public boolean isBigDecimal() {
        return type == ProtoPolyType.DECIMAL;
    }


    @NotNull
    public PolyBigDecimal asBigDecimal() throws ProtoInterfaceServiceException {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }

        throw cannotParse( this, PolyBigDecimal.class );
    }


    public boolean isFloat() {
        return type == ProtoPolyType.FLOAT;
    }


    @NotNull
    public PolyFloat asFloat() throws ProtoInterfaceServiceException {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }

        throw cannotParse( this, PolyFloat.class );
    }


    public boolean isDouble() {
        return type == ProtoPolyType.DOUBLE;
    }


    @NotNull
    public PolyDouble asDouble() throws ProtoInterfaceServiceException {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }

        throw cannotParse( this, PolyDouble.class );
    }


    public boolean isLong() {
        return type == ProtoPolyType.BIGINT;
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
        return type == ProtoPolyType.DATE;
    }


    @NotNull
    public PolyDate asDate() throws ProtoInterfaceServiceException {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw cannotParse( this, PolyDate.class );
    }


    public boolean isTime() {
        return type == ProtoPolyType.TIME;
    }


    @NotNull
    public PolyTime asTime() throws ProtoInterfaceServiceException {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw cannotParse( this, PolyTime.class );
    }


    public boolean isTimestamp() {
        return type == ProtoPolyType.TIMESTAMP;
    }


    @NotNull
    public PolyTimeStamp asTimeStamp() throws ProtoInterfaceServiceException {
        if ( isTimestamp() ) {
            return (PolyTimeStamp) this;
        }

        throw cannotParse( this, PolyTimeStamp.class );
    }


    public boolean isMap() {
        return type == ProtoPolyType.MAP;
    }


    @NotNull
    public PolyMap<PolyValue, PolyValue> asMap() throws ProtoInterfaceServiceException {
        if ( isMap() || isDocument() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw cannotParse( this, PolyMap.class );
    }


    public boolean isEdge() {
        return type == ProtoPolyType.EDGE;
    }


    @NotNull
    public PolyEdge asEdge() throws ProtoInterfaceServiceException {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw cannotParse( this, PolyEdge.class );
    }


    public boolean isNode() {
        return type == ProtoPolyType.NODE;
    }


    @NotNull
    public PolyNode asNode() throws ProtoInterfaceServiceException {
        if ( isNode() ) {
            return (PolyNode) this;
        }
        throw cannotParse( this, PolyNode.class );
    }


    public boolean isPath() {
        return type == ProtoPolyType.PATH;
    }


    public boolean isGraph() {
        return type == ProtoPolyType.GRAPH;
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
        return type == ProtoPolyType.SYMBOL;
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
        return ProtoPolyType.USER_DEFINED_TYPE == type;
    }


    public PolyUserDefinedValue asUserDefinedValue() throws ProtoInterfaceServiceException {
        if ( isUserDefinedValue() ) {
            return (PolyUserDefinedValue) this;
        }
        throw cannotParse( this, PolyUserDefinedValue.class );
    }


    public static PolyValue fromProto( ProtoValue protoValue ) {
        switch ( protoValue.getValueCase() ) {
            case BOOLEAN:
                return new PolyBoolean( protoValue.getBoolean().getBoolean() );
            case INTEGER:
                return new PolyInteger( protoValue.getInteger().getInteger() );
            case LONG:
                return new PolyLong( protoValue.getLong().getLong() );
            case BIG_DECIMAL:
                return deserializeToPolyBigDecimal( protoValue.getBigDecimal() );
            case FLOAT:
                return new PolyFloat( protoValue.getFloat().getFloat() );
            case DOUBLE:
                return new PolyDouble( protoValue.getDouble().getDouble() );
            case DATE:
                return new PolyDate( protoValue.getDate().getDate() );
            case TIME:
                return new PolyTime( protoValue.getTime().getTime() );
            case TIMESTAMP:
                return new PolyTimeStamp( protoValue.getTimestamp().getTimestamp() );
            case INTERVAL:
                switch ( protoValue.getInterval().getUnitCase() ) {
                    case MILLISECONDS:
                        return new PolyInterval( protoValue.getInterval().getMilliseconds(), Unit.MILLISECONDS );
                    case MONTHS:
                        return new PolyInterval( protoValue.getInterval().getMonths(), Unit.MONTHS );
                }
            case STRING:
                return new PolyString( protoValue.getString().getString() );
            case BINARY:
                return new PolyBinary( protoValue.getBinary().getBinary().toByteArray() );
            case NULL:
                return new PolyNull();
            case LIST:
                return deserializeToPolyList( protoValue.getList() );
            case MAP:
                return deserializeToPolyMap( protoValue.getMap() );
            case DOCUMENT:
                return deserializeToPolyDocument( protoValue.getDocument() );
        }
        throw new RuntimeException( "Should never be thrown." );
    }


    public static PolyDocument deserializeToPolyDocument( ProtoDocument document ) {
        return new PolyDocument( deserializeToPolyMap( document.getEntriesList() ) );
    }


    private static PolyMap<PolyString, PolyValue> deserializeToPolyMap( ProtoMap map ) {
        return deserializeToPolyMap( map.getEntriesList() );
    }


    private static PolyMap<PolyString, PolyValue> deserializeToPolyMap( List<ProtoEntry> entries ) {
        return new PolyMap<>( entries.stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .collect( Collectors.toMap(
                        e -> {
                            try {
                                return PolyValue.fromProto( e.getKey() ).asString();
                            } catch ( ProtoInterfaceServiceException ex ) {
                                throw new RuntimeException( "Should never be thrown." );
                            }
                        },
                        e -> PolyValue.fromProto( e.getValue() ),
                        ( key1, key2 ) -> key1
                ) ) );
    }


    private static PolyValue deserializeToPolyList( ProtoList list ) {
        return new PolyList<>( list.getValuesList().stream()
                .map( PolyValue::fromProto )
                .collect( Collectors.toList() ) );
    }


    private static PolyBigDecimal deserializeToPolyBigDecimal( ProtoBigDecimal bigDecimal ) {
        return new PolyBigDecimal( deserializeToBigDecimal( bigDecimal ) );
    }


    private static BigDecimal deserializeToBigDecimal( ProtoBigDecimal bigDecimal ) {
        MathContext context = new MathContext( bigDecimal.getPrecision() );
        byte[] unscaledValue = bigDecimal.getUnscaledValue().toByteArray();
        return new BigDecimal( new BigInteger( unscaledValue ), bigDecimal.getScale(), context );
    }

}

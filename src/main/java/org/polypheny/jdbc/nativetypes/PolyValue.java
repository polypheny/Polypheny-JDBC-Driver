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
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.nativetypes.category.PolyBlob;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.nativetypes.graph.PolyDictionary;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge.EdgeDirection;
import org.polypheny.jdbc.nativetypes.graph.PolyGraph;
import org.polypheny.jdbc.nativetypes.graph.PolyNode;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;
import org.polypheny.jdbc.proto.ProtoBigDecimal;
import org.polypheny.jdbc.proto.ProtoDocument;
import org.polypheny.jdbc.proto.ProtoEdge;
import org.polypheny.jdbc.proto.ProtoEntry;
import org.polypheny.jdbc.proto.ProtoGraph;
import org.polypheny.jdbc.proto.ProtoGraphPropertyHolder;
import org.polypheny.jdbc.proto.ProtoList;
import org.polypheny.jdbc.proto.ProtoMap;
import org.polypheny.jdbc.proto.ProtoNode;
import org.polypheny.jdbc.proto.ProtoUserDefinedType;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;

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


    public static PolyValue fromProto( ProtoValue protoValue ) {
        switch ( protoValue.getType() ) {
            case UNSPECIFIED:
            case ROW_ID:
            case DISTINCT:
            case STRUCTURED:
            case ROW:
            case OTHER:
            case CURSOR:
            case COLUMN_LIST:
            case DYNAMIC_STAR:
            case GEOMETRY:
            case SYMBOL:
            case JSON:
            case MULTISET:
            case ANY:
            case UNRECOGNIZED:
                throw new RuntimeException( "Should never be thrown." );
            case BOOLEAN:
                return new PolyBoolean( protoValue.getBoolean().getBoolean() );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return new PolyInteger( protoValue.getInteger().getInteger() );
            case BIGINT:
                return new PolyLong( protoValue.getLong().getLong() );
            case DECIMAL:
                return deserializeToPolyBigDecimal( protoValue.getBigDecimal() );
            case REAL:
            case FLOAT:
                return new PolyFloat( protoValue.getFloat().getFloat() );
            case DOUBLE:
                return new PolyDouble( protoValue.getDouble().getDouble() );
            case DATE:
                return new PolyDate( protoValue.getDate().getDate() );
            case TIME:
                return new PolyTime( protoValue.getTime().getValue(), protoValue.getTime().getTimeUnit() );
            case TIME_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException( "Conversion from time with local timezone not yet implemented." );
            case TIMESTAMP:
                return new PolyTimeStamp(protoValue.getTimeStamp().getTimeStamp());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                throw new NotImplementedException( "Conversion from timestamp with local timezone not yet implemented." );
            case INTERVAL_SECOND:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY:
            case INTERVAL_MONTH:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_YEAR:
                BigDecimal value = deserializeToBigDecimal( protoValue.getInterval().getValue() );
                return new PolyInterval( value, protoValue.getType() );
            case CHAR:
            case VARCHAR:
                return new PolyString( protoValue.getString().getString() );
            case BINARY:
            case VARBINARY:
                return new PolyBinary( protoValue.getBinary().getBinary().toByteArray() );
            case NULL:
                return new PolyNull();
            case ARRAY:
                return deserializeToPolyList( protoValue.getList() );
            case MAP:
                return deserializeToPolyMap( protoValue.getMap() );
            case DOCUMENT:
                return deserializeToPolyDocument( protoValue.getDocument() );
            case GRAPH:
                return deserializeToPolyGraph( protoValue.getGraph() );
            case NODE:
                return deserializeToPolyNode( protoValue.getNode() );
            case EDGE:
                return deserializeToPolyEdge( protoValue.getEdge() );
            case PATH:
                throw new NotImplementedException( "Conversion from path with local timezone not yet implemented." );
            case IMAGE:
            case VIDEO:
            case AUDIO:
            case FILE:
                return new PolyBinary( protoValue.getBinary().getBinary().toByteArray(), protoValue.getType() );
            case USER_DEFINED_TYPE:
                return deserializeToPolyUserDefinedType( protoValue.getUserDefinedType() );
        }
        throw new RuntimeException("Should never be thrown.");
    }


    private static PolyValue deserializeToPolyUserDefinedType( ProtoUserDefinedType userDefinedType ) {
        Map<String, PolyValue> values = userDefinedType.getValueMap().entrySet().stream()
                .collect( Collectors.toMap( Entry::getKey, e -> PolyValue.fromProto( e.getValue() ), ( key1, key2 ) -> key1 )
                );
        return new PolyUserDefinedValue( userDefinedType.getTemplateMap(), values );
    }


    private static PolyValue deserializeToPolyEdge( ProtoEdge edge ) {
        return new PolyEdge(
                new PolyString( edge.getGraphPropertyHolder().getId().getString() ),
                new PolyDictionary( deserializeToPolyMap( edge.getGraphPropertyHolder().getProperties() ) ),
                getLabels( edge.getGraphPropertyHolder() ),
                new PolyString( edge.getSource().getString() ),
                new PolyString( edge.getTarget().getString() ),
                EdgeDirection.valueOf( edge.getEdgeDirection().name() ),
                new PolyString( edge.getGraphPropertyHolder().getVariableName().getString() )
        );
    }


    private static List<PolyString> getLabels( ProtoGraphPropertyHolder propertyHolder ) {
        return propertyHolder.getLabels().getValuesList().stream()
                .map( p -> p.getString().getString() )
                .map( PolyString::new )
                .collect( Collectors.toList() );
    }


    private static PolyValue deserializeToPolyNode( ProtoNode node ) {
        return new PolyNode(
                new PolyString( node.getGraphPropertyHolder().getId().getString() ),
                new PolyDictionary( deserializeToPolyMap( node.getGraphPropertyHolder().getProperties() ) ),
                getLabels( node.getGraphPropertyHolder() ),
                new PolyString( node.getGraphPropertyHolder().getVariableName().getString() )
        );
    }


    private static PolyValue deserializeToPolyGraph( ProtoGraph graph ) {
        Map<PolyString, PolyNode> nodes = getNodeMap( graph.getNodes() );
        Map<PolyString, PolyEdge> edges = getEdgeMap( graph.getEdges() );
        return new PolyGraph(
                new PolyString( graph.getId().getString() ),
                nodes,
                edges
        );
    }


    private static Map<PolyString, PolyEdge> getEdgeMap( ProtoMap edges ) {
        return edges.getEntriesList().stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .filter( e -> e.getValue().getValueCase() == ValueCase.EDGE )
                .collect( Collectors.toMap(
                                e -> {
                                    try {
                                        return PolyValue.fromProto( e.getKey() ).asString();
                                    } catch ( ProtoInterfaceServiceException ex ) {
                                        throw new RuntimeException( "Should never be thrown." );
                                    }
                                },
                                e -> {
                                    try {
                                        return PolyValue.fromProto( e.getValue() ).asEdge();
                                    } catch ( ProtoInterfaceServiceException ex ) {
                                        throw new RuntimeException( "Should never be thrown." );
                                    }
                                },
                                ( key1, key2 ) -> key1
                        )
                );
    }


    private static Map<PolyString, PolyNode> getNodeMap( ProtoMap nodes ) {
        return nodes.getEntriesList().stream()
                .filter( e -> e.getKey().getValueCase() == ValueCase.STRING )
                .filter( e -> e.getValue().getValueCase() == ValueCase.NODE )
                .collect( Collectors.toMap(
                                e -> {
                                    try {
                                        return PolyValue.fromProto( e.getKey() ).asString();
                                    } catch ( ProtoInterfaceServiceException ex ) {
                                        throw new RuntimeException( "Should never be thrown." );
                                    }
                                },
                                e -> {
                                    try {
                                        return PolyValue.fromProto( e.getValue() ).asNode();
                                    } catch ( ProtoInterfaceServiceException ex ) {
                                        throw new RuntimeException( "Should never be thrown." );
                                    }
                                },
                                ( key1, key2 ) -> key1
                        )
                );
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

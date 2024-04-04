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

import static org.polypheny.db.protointerface.proto.ProtoValue.ValueCase.STRING;

import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.protointerface.proto.ProtoBigDecimal;
import org.polypheny.db.protointerface.proto.ProtoBinary;
import org.polypheny.db.protointerface.proto.ProtoBoolean;
import org.polypheny.db.protointerface.proto.ProtoDate;
import org.polypheny.db.protointerface.proto.ProtoDocument;
import org.polypheny.db.protointerface.proto.ProtoDouble;
import org.polypheny.db.protointerface.proto.ProtoEntry;
import org.polypheny.db.protointerface.proto.ProtoFloat;
import org.polypheny.db.protointerface.proto.ProtoInteger;
import org.polypheny.db.protointerface.proto.ProtoInterval;
import org.polypheny.db.protointerface.proto.ProtoList;
import org.polypheny.db.protointerface.proto.ProtoLong;
import org.polypheny.db.protointerface.proto.ProtoNull;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.db.protointerface.proto.ProtoString;
import org.polypheny.db.protointerface.proto.ProtoTime;
import org.polypheny.db.protointerface.proto.ProtoTimestamp;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.nativetypes.PolyInterval.Unit;
import org.polypheny.jdbc.nativetypes.category.PolyBlob;
import org.polypheny.jdbc.nativetypes.category.PolyNumber;
import org.polypheny.jdbc.nativetypes.category.PolyTemporal;
import org.polypheny.jdbc.nativetypes.document.PolyDocument;
import org.polypheny.jdbc.nativetypes.graph.PolyEdge;
import org.polypheny.jdbc.nativetypes.graph.PolyGraph;
import org.polypheny.jdbc.nativetypes.graph.PolyNode;
import org.polypheny.jdbc.nativetypes.relational.PolyMap;

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
    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        throw cannotParse( this, PolyBoolean.class );
    }


    @NotNull
    private RuntimeException cannotParse( PolyValue value, Class<?> clazz ) {
        return new RuntimeException(
                String.format( "Cannot parse %s to type %s", value, clazz.getSimpleName() )
        );
    }


    public boolean isInteger() {
        return type == ProtoPolyType.INTEGER;
    }


    @NotNull
    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }

        throw cannotParse( this, PolyInteger.class );
    }


    public boolean isDocument() {
        return type == ProtoPolyType.DOCUMENT;
    }


    @NotNull
    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        throw cannotParse( this, PolyDocument.class );
    }


    public boolean isList() {
        return type == ProtoPolyType.ARRAY;
    }


    @NotNull
    public <T extends PolyValue> PolyList<T> asList() {
        if ( isList() ) {
            return (PolyList<T>) this;
        }
        throw cannotParse( this, PolyList.class );
    }


    public boolean isString() {
        return type == ProtoPolyType.VARCHAR;
    }


    @NotNull
    public PolyString asString() {
        if ( isString() ) {
            return (PolyString) this;
        }
        throw cannotParse( this, PolyString.class );
    }


    public boolean isBinary() {
        return type == ProtoPolyType.BINARY;
    }


    @NotNull
    public PolyBinary asBinary() {
        if ( isBinary() ) {
            return (PolyBinary) this;
        }
        throw cannotParse( this, PolyBinary.class );
    }


    public boolean isBigDecimal() {
        return type == ProtoPolyType.DECIMAL;
    }


    @NotNull
    public PolyBigDecimal asBigDecimal() {
        if ( isBigDecimal() ) {
            return (PolyBigDecimal) this;
        }

        throw cannotParse( this, PolyBigDecimal.class );
    }


    public boolean isFloat() {
        return type == ProtoPolyType.FLOAT;
    }


    @NotNull
    public PolyFloat asFloat() {
        if ( isFloat() ) {
            return (PolyFloat) this;
        }

        throw cannotParse( this, PolyFloat.class );
    }


    public boolean isDouble() {
        return type == ProtoPolyType.DOUBLE;
    }


    @NotNull
    public PolyDouble asDouble() {
        if ( isDouble() ) {
            return (PolyDouble) this;
        }

        throw cannotParse( this, PolyDouble.class );
    }


    public boolean isLong() {
        return type == ProtoPolyType.BIGINT;
    }


    @NotNull
    public PolyLong asLong() {
        if ( isLong() ) {
            return (PolyLong) this;
        }

        throw cannotParse( this, PolyLong.class );
    }


    public boolean isTemporal() {
        return TypeUtils.DATETIME_TYPES.contains( type );
    }


    public PolyTemporal asTemporal() {
        if ( isTemporal() ) {
            return (PolyTemporal) this;
        }
        throw cannotParse( this, PolyTemporal.class );
    }


    public boolean isDate() {
        return type == ProtoPolyType.DATE;
    }


    @NotNull
    public PolyDate asDate() {
        if ( isDate() ) {
            return (PolyDate) this;
        }
        throw cannotParse( this, PolyDate.class );
    }


    public boolean isTime() {
        return type == ProtoPolyType.TIME;
    }


    @NotNull
    public PolyTime asTime() {
        if ( isTime() ) {
            return (PolyTime) this;
        }

        throw cannotParse( this, PolyTime.class );
    }


    public boolean isTimestamp() {
        return type == ProtoPolyType.TIMESTAMP;
    }


    @NotNull
    public PolyTimeStamp asTimeStamp() {
        if ( isTimestamp() ) {
            return (PolyTimeStamp) this;
        }

        throw cannotParse( this, PolyTimeStamp.class );
    }


    public boolean isMap() {
        return type == ProtoPolyType.MAP;
    }


    @NotNull
    public PolyMap<PolyValue, PolyValue> asMap() {
        if ( isMap() || isDocument() ) {
            return (PolyMap<PolyValue, PolyValue>) this;
        }
        throw cannotParse( this, PolyMap.class );
    }


    public boolean isEdge() {
        return type == ProtoPolyType.EDGE;
    }


    @NotNull
    public PolyEdge asEdge() {
        if ( isEdge() ) {
            return (PolyEdge) this;
        }
        throw cannotParse( this, PolyEdge.class );
    }


    public boolean isNode() {
        return type == ProtoPolyType.NODE;
    }


    @NotNull
    public PolyNode asNode() {
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
    public PolyGraph asGraph() {
        if ( isGraph() ) {
            return (PolyGraph) this;
        }
        throw cannotParse( this, PolyGraph.class );
    }


    public boolean isNumber() {
        return TypeUtils.NUMERIC_TYPES.contains( type );
    }


    public PolyNumber asNumber() {
        if ( isNumber() ) {
            return (PolyNumber) this;
        }
        throw cannotParse( this, PolyNumber.class );
    }


    public boolean isInterval() {
        return type == ProtoPolyType.INTERVAL;
    }


    public PolyInterval asInterval() {
        if ( isInterval() ) {
            return (PolyInterval) this;
        }
        throw cannotParse( this, PolyInterval.class );
    }


    public boolean isSymbol() {
        return type == ProtoPolyType.SYMBOL;
    }


    public PolySymbol asSymbol() {
        if ( isSymbol() ) {
            return (PolySymbol) this;
        }
        throw cannotParse( this, PolySymbol.class );
    }


    public boolean isBlob() {
        return TypeUtils.BLOB_TYPES.contains( type );
    }


    @NotNull
    public PolyBlob asBlob() {
        if ( isBlob() ) {
            return (PolyBlob) this;
        }
        throw cannotParse( this, PolyBlob.class );
    }


    public boolean isUserDefinedValue() {
        return ProtoPolyType.USER_DEFINED_TYPE == type;
    }


    public PolyUserDefinedValue asUserDefinedValue() {
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
            case DOCUMENT:
                return deserializeToPolyDocument( protoValue.getDocument() );
        }
        throw new RuntimeException( "Should never be thrown." );
    }


    public static PolyDocument deserializeToPolyDocument( ProtoDocument document ) {
        return new PolyDocument( deserializeToPolyMap( document.getEntriesList() ) );
    }


    private static PolyMap<PolyString, PolyValue> deserializeToPolyMap( List<ProtoEntry> entries ) {
        return new PolyMap<>( entries.stream()
                .filter( e -> e.getKey().getValueCase() == STRING )
                .collect( Collectors.toMap(
                        e -> PolyValue.fromProto( e.getKey() ).asString(),
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


    public ProtoValue toProto() {
        switch ( type ) {
            case BOOLEAN:
                return toProtoBoolean( this.asBoolean() );
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return toProtoInteger( this.asInteger() );
            case BIGINT:
                return toProtoLong( this.asLong() );
            case DECIMAL:
                return toProtoBigDecimal( this.asBigDecimal() );
            case REAL:
            case FLOAT:
                return toProtoFloat( this.asFloat() );
            case DOUBLE:
                return toProtoDouble( this.asDouble() );
            case DATE:
                return toProtoDate( this.asDate() );
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                // Assuming same handling for TIME and TIME_WITH_LOCAL_TIME_ZONE, adjust if needed
                return toProtoTime( this.asTime() );
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                // Assuming same handling for TIMESTAMP and TIMESTAMP_WITH_LOCAL_TIME_ZONE, adjust if needed
                return toProtoTimestamp( this.asTimeStamp() );
            case INTERVAL:
                return toProtoInterval( this.asInterval() );
            case CHAR:
            case VARCHAR:
                return toProtoString( this.asString() );
            case BINARY:
            case VARBINARY:
                return toProtoBinary( this.asBinary() );
            case NULL:
                return toProtoNull(); // Assuming no conversion is needed for NULL type
            case ARRAY:
                // Placeholder for array handling
                return toProtoList( this.asList() );
            case DOCUMENT:
                return toProtoDocument( this.asDocument() );
            // Add additional cases here for other types as needed
            default:
                throw new RuntimeException( "Unsupported type." );
        }
    }


    private ProtoValue toProtoBoolean( PolyBoolean polyBoolean ) {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( polyBoolean.value )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .build();
    }


    private ProtoValue toProtoInteger( PolyInteger polyInteger ) {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( polyInteger.value )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .build();
    }


    private ProtoValue toProtoLong( PolyLong polyLong ) {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( polyLong.value )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .build();
    }


    private ProtoValue toProtoFloat( PolyFloat polyFloat ) {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( polyFloat.value )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .build();
    }


    private ProtoValue toProtoDouble( PolyDouble polyDouble ) {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( polyDouble.value )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .build();
    }


    private ProtoValue toProtoDate( PolyDate polyDate ) {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( polyDate.getDaysSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .build();
    }


    private ProtoValue toProtoTime( PolyTime polyTime ) {
        ProtoTime protoTime = ProtoTime.newBuilder()
                .setTime( polyTime.ofDay )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .build();
    }


    private ProtoValue toProtoTimestamp( PolyTimeStamp polyTimeStamp ) {
        ProtoTimestamp protoTimestamp = ProtoTimestamp.newBuilder()
                .setTimestamp( polyTimeStamp.getMilliSinceEpoch() )
                .build();
        return ProtoValue.newBuilder()
                .setTimestamp( protoTimestamp )
                .build();
    }


    private ProtoValue toProtoInterval( PolyInterval polyInterval ) {
        ProtoInterval.Builder protoInterval = ProtoInterval.newBuilder();
        if ( polyInterval.unit == Unit.MONTHS ) {
            protoInterval.setMonths( polyInterval.value );
        } else {
            protoInterval.setMilliseconds( polyInterval.value );
        }
        return ProtoValue.newBuilder()
                .setInterval( protoInterval.build() )
                .build();
    }


    private ProtoValue toProtoString( PolyString polyString ) {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( polyString.value )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .build();
    }


    private ProtoValue toProtoBinary( PolyBinary polyBinary ) {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( polyBinary.value ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .build();
    }


    private ProtoValue toProtoNull() {
        return ProtoValue.newBuilder().setNull( ProtoNull.newBuilder().build() ).build();
    }


    private ProtoValue toProtoBigDecimal( PolyBigDecimal polyBigDecimal ) {
        BigDecimal bigDecimal = polyBigDecimal.value;
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( bigDecimal.unscaledValue().toByteArray() ) )
                .setScale( bigDecimal.scale() )
                .setPrecision( bigDecimal.precision() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .build();
    }


    private ProtoValue toProtoList( PolyList polyList ) {
        List<ProtoValue> values = ((Stream<PolyValue>) polyList.stream())
                .map( PolyValue::toProto )
                .collect( Collectors.toList() );
        ProtoList protoList = ProtoList.newBuilder()
                .addAllValues( values )
                .build();
        return ProtoValue.newBuilder()
                .setList( protoList )
                .build();
    }


    private ProtoValue toProtoDocument( PolyDocument polyDocument ) {
        List<ProtoEntry> protoEntries = polyDocument.asMap().entrySet().stream().map( polyMapEntry -> {
            ProtoValue protoKey = polyMapEntry.getKey().toProto(); // Reuse serialize() method for consistency
            ProtoValue protoValue = polyMapEntry.getValue().toProto();
            return ProtoEntry.newBuilder()
                    .setKey( protoKey )
                    .setValue( protoValue )
                    .build();
        } ).collect( Collectors.toList() );

        ProtoDocument protoDocument = ProtoDocument.newBuilder()
                .addAllEntries( protoEntries )
                .build();
        return ProtoValue.newBuilder()
                .setDocument( protoDocument )
                .build();
    }

}

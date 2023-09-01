package org.polypheny.jdbc.serialisation;

import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.proto.ProtoBigDecimal;
import org.polypheny.jdbc.proto.ProtoBinary;
import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoDate;
import org.polypheny.jdbc.proto.ProtoDouble;
import org.polypheny.jdbc.proto.ProtoFloat;
import org.polypheny.jdbc.proto.ProtoInteger;
import org.polypheny.jdbc.proto.ProtoList;
import org.polypheny.jdbc.proto.ProtoLong;
import org.polypheny.jdbc.proto.ProtoNull;
import org.polypheny.jdbc.proto.ProtoRowId;
import org.polypheny.jdbc.proto.ProtoString;
import org.polypheny.jdbc.proto.ProtoTime;
import org.polypheny.jdbc.proto.ProtoTime.TimeUnit;
import org.polypheny.jdbc.proto.ProtoTimeStamp;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class ProtoValueSerializer {

    public static Map<String, ProtoValue> serializeParameterMap( Map<String, TypedValue> parameters ) {
        //TODO implement this
        return null;
    }


    public static List<ProtoValue> serializeParameterList( List<TypedValue> values ) {
        return values.stream().map( ProtoValueSerializer::saveSerialize ).collect( Collectors.toList() );
    }


    private static ProtoValue saveSerialize( TypedValue typedValue ) throws RuntimeException {
        try {
            return serialize( typedValue );
        } catch ( SQLException e ) {
            throw new RuntimeException( e.getMessage() );
        }
    }


    public static ProtoValue serialize( TypedValue typedValue ) throws SQLException {
        if (typedValue.isNull()) {
            return serializeAsProtoNull(typedValue);
        }
        switch ( typedValue.getJdbcType() ) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return serializeAsProtoInteger( typedValue );
            case Types.BIT:
            case Types.BOOLEAN:
                return serializeAsProtoBoolean( typedValue );
            case Types.BIGINT:
            case Types.DOUBLE:
                return serializeAsProtoLong( typedValue );
            case Types.FLOAT:
                return serializeAsProtoFloat( typedValue );
            case Types.REAL:
                return serializeAsProtoDouble( typedValue );
            case Types.NUMERIC:
            case Types.DECIMAL:
                return serializeAsProtoBigDecimal( typedValue );
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return serializeAsProtoString( typedValue );
            case Types.DATE:
                return serializeAsProtoDate( typedValue );
            case Types.TIME:
                return serializeAsProtoTime( typedValue );
            case Types.TIMESTAMP:
                return serializeAsTimeStamp( typedValue );
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB:
                // requires getter conversions to work propertly...
                return serializeAsProtoBinary( typedValue );
            case Types.NULL:
                return serializeAsProtoNull( typedValue );
            case Types.OTHER:
                // TODO TH: find something useful to do here...
                break;
            case Types.JAVA_OBJECT:
                // TODO TH: find something useful to do here...
                break;
            case Types.DISTINCT:
                // TODO TH: find something useful to do here...
                break;
            case Types.STRUCT:
                // TODO TH: find something useful to do here...
                break;
            case Types.ARRAY:
                return serializeAsProtoList( typedValue );
            case Types.CLOB:
                // TODO TH: find something useful to do here...
                break;
            case Types.REF:
                // TODO TH: find something useful to do here...
                break;
            case Types.DATALINK:
                // TODO TH: find something useful to do here...
                break;
            case Types.ROWID:
                return serializeAsProtoRowId( typedValue );
            case Types.NCLOB:
                // TODO TH: find something useful to do here...
                break;
            case Types.SQLXML:
                // TODO TH: find something useful to do here...
                break;
            case Types.REF_CURSOR:
                // TODO TH: find something useful to do here...
                break;
            case Types.TIME_WITH_TIMEZONE:
                // TODO TH: find something useful to do here...
                break;
            case Types.TIMESTAMP_WITH_TIMEZONE:
                // TODO TH: find something useful to do here...
                break;
        }
        throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.DATA_TYPE_MISSMATCH, "Serialization of jdbc type " + typedValue.getJdbcType() + " not known" );
    }


    private static ProtoValue serializeAsProtoRowId( TypedValue typedValue ) throws SQLException {
        ProtoRowId protoRowId = ProtoRowId.newBuilder()
                .setRowId( typedValue.asRowId().toString() )
                .build();
        return ProtoValue.newBuilder()
                .setRowId( protoRowId )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoList( TypedValue typedValue ) throws SQLException {
        List<ProtoValue> elements = new ArrayList<>();
        for ( Object object : (Object[]) typedValue.asArray().getArray() ) {
            elements.add( ProtoValueSerializer.serialize( TypedValue.fromObject( object ) ) );
        }
        ProtoList protoList = ProtoList.newBuilder()
                .addAllValues( elements )
                .build();
        return ProtoValue.newBuilder()
                .setList( protoList )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue.ProtoValueType getType( TypedValue typedValue ) {
        return JdbcToProtoTypeMap.getTypeOf( typedValue );
    }


    private static ProtoValue serializeAsProtoDouble( TypedValue typedValue ) throws SQLException {
        ProtoDouble protoDouble = ProtoDouble.newBuilder()
                .setDouble( typedValue.asDouble() )
                .build();
        return ProtoValue.newBuilder()
                .setDouble( protoDouble )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoFloat( TypedValue typedValue ) throws SQLException {
        ProtoFloat protoFloat = ProtoFloat.newBuilder()
                .setFloat( typedValue.asFloat() )
                .build();
        return ProtoValue.newBuilder()
                .setFloat( protoFloat )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoLong( TypedValue typedValue ) throws SQLException {
        ProtoLong protoLong = ProtoLong.newBuilder()
                .setLong( typedValue.asLong() )
                .build();
        return ProtoValue.newBuilder()
                .setLong( protoLong )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoBigDecimal( TypedValue typedValue ) throws SQLException {
        BigDecimal bigDecimal = typedValue.asBigDecimal();
        ProtoBigDecimal protoBigDecimal = ProtoBigDecimal.newBuilder()
                .setUnscaledValue( ByteString.copyFrom( bigDecimal.unscaledValue().toByteArray() ) )
                .setScale( bigDecimal.scale() )
                .setPrecision( bigDecimal.precision() )
                .build();
        return ProtoValue.newBuilder()
                .setBigDecimal( protoBigDecimal )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoDate( TypedValue typedValue ) throws SQLException {
        ProtoDate protoDate = ProtoDate.newBuilder()
                .setDate( typedValue.asDate().getTime() / 86400000 )  // 86400000 = Milliseconds in a day = 24 * 60 * 60 * 1000
                .build();
        return ProtoValue.newBuilder()
                .setDate( protoDate )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoString( TypedValue typedValue ) throws SQLException {
        ProtoString protoString = ProtoString.newBuilder()
                .setString( typedValue.asString() )
                .build();
        return ProtoValue.newBuilder()
                .setString( protoString )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoTime( TypedValue typedValue ) throws SQLException {
        ProtoTime protoTime = ProtoTime.newBuilder()
                // TODO TH: Fix this. Why is this an int32?!
                .setValue( (int)typedValue.asTime().getTime() )
                .setTimeUnit( TimeUnit.MILLISECOND )
                .build();
        return ProtoValue.newBuilder()
                .setTime( protoTime )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsTimeStamp( TypedValue typedValue ) throws SQLException {
        ProtoTimeStamp protoTimeStamp = ProtoTimeStamp.newBuilder()
                .setTimeStamp( typedValue.asTimestamp().getTime() )
                .build();
        return ProtoValue.newBuilder()
                .setTimeStamp( protoTimeStamp )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoBinary( TypedValue typedValue ) throws SQLException {
        ProtoBinary protoBinary = ProtoBinary.newBuilder()
                .setBinary( ByteString.copyFrom( typedValue.asBytes() ) )
                .build();
        return ProtoValue.newBuilder()
                .setBinary( protoBinary )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoNull( TypedValue typedValue ) {
        return ProtoValue.newBuilder()
                .setNull( ProtoNull.newBuilder().build() )
                .setType( getType( typedValue ) )
                .build();
    }


    private static ProtoValue serializeAsProtoBoolean( TypedValue typedValue ) throws SQLException {
        ProtoBoolean protoBoolean = ProtoBoolean.newBuilder()
                .setBoolean( typedValue.asBoolean() )
                .build();
        return ProtoValue.newBuilder()
                .setBoolean( protoBoolean )
                .setType( ProtoValueType.NULL )
                .build();
    }


    private static ProtoValue serializeAsProtoInteger( TypedValue typedValue ) throws SQLException {
        ProtoInteger protoInteger = ProtoInteger.newBuilder()
                .setInteger( typedValue.asInt() )
                .build();
        return ProtoValue.newBuilder()
                .setInteger( protoInteger )
                .setType( getType( typedValue ) )
                .build();
    }

}

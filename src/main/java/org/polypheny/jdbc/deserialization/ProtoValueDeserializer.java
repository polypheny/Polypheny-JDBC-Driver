package org.polypheny.jdbc.deserialization;


import com.google.protobuf.ByteString;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.proto.ProtoInterval;
import org.polypheny.db.protointerface.proto.ProtoInterval.UnitCase;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.PolyphenyArray;
import org.polypheny.jdbc.jdbctypes.TypedValue;
import org.polypheny.jdbc.nativetypes.PolyInterval;
import org.polypheny.jdbc.nativetypes.PolyInterval.Unit;
import org.polypheny.jdbc.nativetypes.PolyValue;

public class ProtoValueDeserializer {

    private static final long MILLISECONDS_PER_DAY = 24 * 60 * 60 * 1000;


    public static TypedValue deserializeToTypedValue( ProtoValue value ) {
        // TODO Java 17: Convert to switch expression
        try {
            switch ( value.getValueCase() ) {
                case BOOLEAN:
                    return TypedValue.fromBoolean( value.getBoolean().getBoolean() );
                case INTEGER:
                    return TypedValue.fromInteger( value.getInteger().getInteger() );
                case LONG:
                    return TypedValue.fromLong( value.getLong().getLong() );
                case BINARY:
                    return TypedValue.fromObject( value.getBinary().getBinary(), Types.BINARY );
                case DATE:
                    return TypedValue.fromDate( new Date( value.getDate().getDate() * MILLISECONDS_PER_DAY ) );
                case DOUBLE:
                    return TypedValue.fromDouble( value.getDouble().getDouble() );
                case FLOAT:
                    // according to jdbc appendix B.1 floats should internally be represented as doubles
                    return TypedValue.fromDouble( value.getFloat().getFloat() );
                case NULL:
                    return TypedValue.fromNull( Types.NULL );
                case STRING:
                    return TypedValue.fromString( value.getString().getString() );
                case TIME:
                    return TypedValue.fromTime( new Time( value.getTime().getTime() ) );
                case TIMESTAMP:
                    return TypedValue.fromTimestamp( new Timestamp( value.getTimestamp().getTimestamp() ) );
                case BIG_DECIMAL:
                    return TypedValue.fromBigDecimal( getBigDecimal( value.getBigDecimal().getUnscaledValue(), value.getBigDecimal().getScale() ) );
                case LIST:
                    return TypedValue.fromArray( getArray( value ) );
                case INTERVAL:
                    return TypedValue.fromPolyValue( getInterval( value.getInterval() ) );
                case DOCUMENT:
                default:
                    throw new RuntimeException( "Cannot deserialize ProtoValue of case " + value.getValueCase() );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

    private static PolyInterval getInterval( ProtoInterval interval ) {
        if ( interval.getUnitCase() == UnitCase.MILLISECONDS ) {
            return new PolyInterval( interval.getMilliseconds(), Unit.MILLISECONDS );
        } else {
            return new PolyInterval( interval.getMilliseconds(), Unit.MONTHS );
        }
    }


    private static Array getArray( ProtoValue value ) throws SQLException {
        String baseType = value.getValueCase().name();
        List<TypedValue> values = value.getList().getValuesList().stream()
                .map( ProtoValueDeserializer::deserializeToTypedValue )
                .collect( Collectors.toList() );
        return new PolyphenyArray( baseType, values );
    }


    public static BigDecimal getBigDecimal( ByteString unscaledValue, int scale ) {
        BigInteger value = new BigInteger( unscaledValue.toByteArray() );
        return new BigDecimal( value, scale );
    }

}

package org.polypheny.jdbc.deserialization;


import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Types;
import java.time.LocalTime;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class ProtoValueDeserializer {

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
                    return TypedValue.fromDate(new Date( value.getDate().getDate()));
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
                    return TypedValue.fromTime( new Time(value.getTime().getTime()));
                case TIMESTAMP:
                    return TypedValue.fromObject( value.getTimestamp().getTimestamp(), Types.TIMESTAMP );
                case BIG_DECIMAL:
                case INTERVAL:
                case USER_DEFINED_TYPE:
                case LIST:
                case MAP:
                case DOCUMENT:
                case NODE:
                case EDGE:
                case PATH:
                case GRAPH:
                case ROW_ID:
                default:
                    throw new RuntimeException( "Cannot deserialize ProtoValue of case " + value.getValueCase() );
            }
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

}

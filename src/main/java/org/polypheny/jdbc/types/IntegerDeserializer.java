package org.polypheny.jdbc.types;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoInteger;
import org.polypheny.jdbc.proto.ProtoValue;

public class IntegerDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.TINYINT:
                return deserializeToByte( value.getInteger() );
            case Types.SMALLINT:
                return deserializeToShort( value.getInteger() );
            case Types.INTEGER:
                return deserializeToInt( value.getInteger() );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto integer." );
    }


    private static byte deserializeToByte( ProtoInteger protoInteger ) {
        return Integer.valueOf( protoInteger.getInteger() ).byteValue();
    }


    private static short deserializeToShort( ProtoInteger protoInteger ) {
        return Integer.valueOf( protoInteger.getInteger() ).shortValue();
    }


    private static int deserializeToInt( ProtoInteger protoInteger ) {
        return protoInteger.getInteger();
    }


}

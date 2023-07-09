package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class BooleanDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch (jdbcType) {
            case Types.BOOLEAN:
                boolean b = deserializeToBoolean( value.getBoolean() );
                return TypedValue.fromObject( b, ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto boolean." );
    }


    private static boolean deserializeToBoolean( ProtoBoolean protoBoolean ) {
        return protoBoolean.getBoolean();
    }

}

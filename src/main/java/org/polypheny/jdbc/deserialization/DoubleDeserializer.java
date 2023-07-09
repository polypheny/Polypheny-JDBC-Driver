package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoDouble;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class DoubleDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.DOUBLE:
                Double d = deserializeToDouble( value.getDouble() );
                return TypedValue.fromObject( d, jdbcType );

        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto double." );
    }


    private static Double deserializeToDouble( ProtoDouble protoDouble ) {
        return protoDouble.getDouble();
    }

}

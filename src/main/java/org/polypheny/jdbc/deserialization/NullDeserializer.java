package org.polypheny.jdbc.deserialization;

import org.polypheny.jdbc.proto.ProtoValue;
import java.sql.Types;
import org.polypheny.jdbc.types.TypedValue;

public class NullDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch (jdbcType) {
            case Types.NULL:
                return TypedValue.fromNull( jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto null." );
    }

}

package org.polypheny.jdbc.deserialization;

import org.polypheny.db.protointerface.proto.ProtoValue;
import java.sql.Types;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class NullDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.NULL:
                return TypedValue.fromNull( jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto null." );
    }

}

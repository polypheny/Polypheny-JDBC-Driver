package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class UserDefinedTypeDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.OTHER:
                return null;
                //TODO implementation
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto user defined type." );
    }

}

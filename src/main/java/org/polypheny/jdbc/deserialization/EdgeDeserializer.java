package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class EdgeDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.JAVA_OBJECT:
                return null;
            //TODO implementation
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto edge." );
    }

}

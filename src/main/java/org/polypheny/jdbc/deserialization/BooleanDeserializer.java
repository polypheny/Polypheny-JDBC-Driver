package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.db.protointerface.proto.ProtoBoolean;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class BooleanDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
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

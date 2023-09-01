package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoDouble;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class DoubleDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
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

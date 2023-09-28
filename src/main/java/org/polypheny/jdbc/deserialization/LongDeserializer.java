package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.db.protointerface.proto.ProtoLong;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class LongDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.BIGINT:
                Long l = deserializeToLong( value.getLong() );
                return TypedValue.fromObject( l, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto long." );
    }


    private Long deserializeToLong( ProtoLong protoLong ) {
        return protoLong.getLong();
    }

}

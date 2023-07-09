package org.polypheny.jdbc.deserialization;

import java.sql.Time;
import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoTime;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.Type;
import org.polypheny.jdbc.types.TypedValue;

public class TimeDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.TIME:
                Time t = deserializeToSqlTime( value.getTime() );
                return TypedValue.fromObject( t, jdbcType );
            case Types.OTHER:
                throw new NotImplementedException("Deserialization of time with local timezone not implemented yet.");
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto time." );
    }


    private Time deserializeToSqlTime( ProtoTime protoTime ) {
        return new Time( protoTime.getValue());
    }

}

package org.polypheny.jdbc.types;

import java.sql.Time;
import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoTime;
import org.polypheny.jdbc.proto.ProtoValue;

public class TimeDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.TIME:
                return deserializeToSqlTime( value.getTime() );
            case Types.OTHER:
                throw new NotImplementedException("Deserialization of time with local timezone not implemented yet.");
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto time." );
    }


    private Object deserializeToSqlTime( ProtoTime protoTime ) {
        return new Time( protoTime.getValue());
    }

}

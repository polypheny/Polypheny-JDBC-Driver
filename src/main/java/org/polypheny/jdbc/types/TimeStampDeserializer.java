package org.polypheny.jdbc.types;

import java.sql.Timestamp;
import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoTimeStamp;
import org.polypheny.jdbc.proto.ProtoValue;

public class TimeStampDeserializer implements ValueDeserializer {

    @Override
    public Object deserilize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.TIME:
                return deserializeToSqlTimeStamp( value.getTimeStamp() );
            case Types.OTHER:
                throw new NotImplementedException( "Deserialization of timestamps with local timezone not implemented yet." );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto time." );
    }


    private Object deserializeToSqlTimeStamp( ProtoTimeStamp protoTimeStamp ) {
        return new Timestamp( protoTimeStamp.getTimeStamp() );
    }

}

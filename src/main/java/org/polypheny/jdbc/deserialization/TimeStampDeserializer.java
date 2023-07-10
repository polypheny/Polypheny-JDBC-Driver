package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoTimeStamp;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class TimeStampDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.TIMESTAMP:
                Timestamp t = deserializeToSqlTimeStamp( value.getTimeStamp() );
                return TypedValue.fromObject( t, jdbcType );
            case Types.OTHER:
                throw new NotImplementedException( "Deserialization of timestamps with local timezone not implemented yet." );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto time." );
    }


    private Timestamp deserializeToSqlTimeStamp( ProtoTimeStamp protoTimeStamp ) {
        return new Timestamp( protoTimeStamp.getTimeStamp() );
    }

}

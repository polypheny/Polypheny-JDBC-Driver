package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.protointerface.proto.ProtoTimeStamp;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class TimeStampDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
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

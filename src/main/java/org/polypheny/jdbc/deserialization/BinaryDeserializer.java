package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoBinary;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class BinaryDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType) {
            case Types.BINARY:
            case Types.VARBINARY:
               byte[] b = deserilizeToByteArray( value.getBinary() );
               return TypedValue.fromObject(b, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto string." );
    }


    private byte[] deserilizeToByteArray( ProtoBinary protoBinary ) {
        return protoBinary.getBinary().toByteArray();
    }

}
package org.polypheny.jdbc.types;

import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoBinary;
import org.polypheny.jdbc.proto.ProtoValue;

public class BinaryDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.BINARY:
            case Types.VARBINARY:
                return deserilizeToByteArray( value.getBinary() );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto string." );
    }


    private Object deserilizeToByteArray( ProtoBinary protoBinary ) {
        return protoBinary.getBinary().toByteArray();
    }

}

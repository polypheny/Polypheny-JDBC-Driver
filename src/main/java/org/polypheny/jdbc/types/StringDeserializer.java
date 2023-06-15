package org.polypheny.jdbc.types;

import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoString;
import org.polypheny.jdbc.proto.ProtoValue;

public class StringDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.CHAR:
            case Types.VARCHAR:
                return deserilizeToString( value.getString() );
            case Types.BINARY:
            case Types.VARBINARY:
                return deserilaizeToByteArray( value.getString() );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto string." );
    }


    private String deserilizeToString( ProtoString protoString ) {
        return protoString.getString();
    }


    private byte[] deserilaizeToByteArray( ProtoString protoString ) {
        //TODO TH: Find charset that allows unambiguous conversion to byte array. Needs to match with PolyString on DB side.
        throw new NotImplementedException( "Deserialization fomr string to bytes is not supportet yet." );
    }

}

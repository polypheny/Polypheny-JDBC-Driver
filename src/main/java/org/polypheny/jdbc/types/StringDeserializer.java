package org.polypheny.jdbc.types;

import java.sql.Types;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoString;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;

public class StringDeserializer implements ValueDeserializer {
    private static final ProtoValue.ValueCase TARGET_CASE = ValueCase.STRING;

    @Override
    public boolean deserializes( ProtoValue value ) {
        return value.getValueCase() == TARGET_CASE;
    }


    @Override
    public Object deserilize( ProtoValue value ) {
        switch ( ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) ) {
            case Types.CHAR:
            case Types.VARCHAR:
                return deserilizeToString(value.getString());
            case Types.BINARY:
            case Types.VARBINARY:
                return deserilaizeToByteArray(value.getString());
        }
        throw new IllegalArgumentException("Illegal jdbc type for proto string.");
    }

    private String deserilizeToString( ProtoString protoString ) {
        return protoString.getString();
    }

    private byte[] deserilaizeToByteArray(ProtoString protoString) {
        //TODO TH: Find charset that allows unambiguous conversion to byte array. Needs to match with PolyString on DB side.
        throw new NotImplementedException("Deserialization fomr string to bytes is not supportet yet.");
    }
}

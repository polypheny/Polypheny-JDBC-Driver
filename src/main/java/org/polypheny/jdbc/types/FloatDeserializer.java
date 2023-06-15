package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoFloat;
import org.polypheny.jdbc.proto.ProtoValue;

public class FloatDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToFloat( value.getFloat() );
    }


    private Object deserializeToFloat( ProtoFloat protoFloat ) {
        return protoFloat.getFloat();
    }

}

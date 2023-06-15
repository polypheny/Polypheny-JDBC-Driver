package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoValue;

public class BooleanDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToBoolean( value.getBoolean() );
    }


    private static boolean deserializeToBoolean( ProtoBoolean protoBoolean ) {
        return protoBoolean.getBoolean();
    }

}

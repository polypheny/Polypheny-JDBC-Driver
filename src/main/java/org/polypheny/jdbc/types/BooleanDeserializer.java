package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;

public class BooleanDeserializer implements ValueDeserializer {

    private static final ProtoValue.ValueCase TARGET_CASE = ValueCase.BOOLEAN;


    @Override
    public Object deserilize( ProtoValue value ) {
        return deserializeToBoolean( value.getBoolean() );
    }


    private static boolean deserializeToBoolean( ProtoBoolean protoBoolean ) {
        return protoBoolean.getBoolean();
    }

}

package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;

public class BooleanDeserializer implements ValueDeserializer {

    private static final ProtoValue.ValueCase TARGET_CASE = ValueCase.BOOLEAN;


    @Override
    public boolean deserializes( ProtoValue value ) {
        return value.getValueCase() == TARGET_CASE;
    }


    @Override
    public Object deserilize( ProtoValue value ) {
        return deserializeToBoolean( value.getBoolean() );
    }

    private static boolean deserializeToBoolean( ProtoBoolean protoBoolean ) {
        return protoBoolean.getBoolean();
    }

}

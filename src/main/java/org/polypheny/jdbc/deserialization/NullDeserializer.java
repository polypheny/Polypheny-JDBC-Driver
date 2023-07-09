package org.polypheny.jdbc.deserialization;

import org.polypheny.jdbc.proto.ProtoValue;

public class NullDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return null;
    }

}

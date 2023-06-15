package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoValue;

public class NullDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return null;
    }

}

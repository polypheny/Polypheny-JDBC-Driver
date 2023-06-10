package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoValue;

public class NullDeserializer implements ValueDeserializer {

    @Override
    public Object deserilize( ProtoValue value ) {
        return null;
    }

}
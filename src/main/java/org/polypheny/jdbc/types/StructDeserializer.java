package org.polypheny.jdbc.types;

import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoStructured;
import org.polypheny.jdbc.proto.ProtoValue;

public class StructDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToStruct( value.getStructured() );
    }

    private String deserializeToStruct( ProtoStructured protoStructured ) {
        throw new NotImplementedException("Struct deserialization is not supported yet");
    }
}

package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoLong;
import org.polypheny.jdbc.proto.ProtoValue;

public class LongDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToLong( value.getLong() );
    }


    private Object deserializeToLong( ProtoLong protoLong ) {
        return protoLong.getLong();
    }

}

package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoValue;

public interface ValueDeserializer {
    Object deserialize( ProtoValue value );

}

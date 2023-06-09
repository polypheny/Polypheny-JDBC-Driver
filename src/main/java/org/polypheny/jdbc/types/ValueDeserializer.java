package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoValue;

public interface ValueDeserializer {
    boolean deserializes(ProtoValue value);

    Object deserilize( ProtoValue value );

}

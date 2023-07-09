package org.polypheny.jdbc.deserialization;

import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public interface ValueDeserializer {
    TypedValue deserialize( ProtoValue value );

}

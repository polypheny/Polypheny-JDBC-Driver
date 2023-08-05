package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public interface ValueDeserializer {
    TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException;
}

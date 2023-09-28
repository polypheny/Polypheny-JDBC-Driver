package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public interface ValueDeserializer {
    TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException;
}

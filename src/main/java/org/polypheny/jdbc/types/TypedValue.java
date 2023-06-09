package org.polypheny.jdbc.types;

import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.utils.ProtoValueDeserializer;

public class TypedValue {
    private final int jdbcType;
    private final Object value;

    public TypedValue( ProtoValue protoValue ) {
        this.jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( protoValue.getType() );
        this.value = ProtoValueDeserializer.deserialize( protoValue );
    }
}

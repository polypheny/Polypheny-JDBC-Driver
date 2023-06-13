package org.polypheny.jdbc.types;

import lombok.Getter;
import org.polypheny.jdbc.proto.ProtoValue;

public class TypedValue {
    private final int jdbcType;
    @Getter
    private final Object value;

    public TypedValue( ProtoValue protoValue ) {
        this.jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( protoValue.getType() );
        this.value = ProtoValueDeserializer.deserialize( protoValue );
    }
}

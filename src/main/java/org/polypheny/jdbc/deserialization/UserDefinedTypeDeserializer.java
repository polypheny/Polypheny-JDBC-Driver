package org.polypheny.jdbc.deserialization;

import org.polypheny.jdbc.proto.ProtoUserDefinedType;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

import java.sql.Types;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class UserDefinedTypeDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue(ProtoValue value) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto(value.getType());
        switch (jdbcType) {
            case Types.OTHER:
                return deserializeToUdtPrototype(value.getUserDefinedType(), value.getType().name());
        }
        throw new IllegalArgumentException("Illegal jdbc type for proto user defined type.");
    }

    private TypedValue deserializeToUdtPrototype(ProtoUserDefinedType userDefinedType, String typeName) {
        ArrayList<TypedValue> values = userDefinedType.getValueMap().values().stream()
                .map(ProtoValueDeserializer::deserializeToTypedValue )
                .collect(Collectors.toCollection(ArrayList::new));
        return TypedValue.fromUdtPrototype(new UDTPrototype(typeName, values));
    }
}

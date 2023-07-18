package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.polypheny.jdbc.proto.ProtoList;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class ListDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.ARRAY:
                // we deserialize this as an udt because arrays can't have mixed element types
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto list." );
    }

    private TypedValue asUdtPrototype(ProtoList list, String typeName) {
        ArrayList<TypedValue> values = list.getValuesList().stream()
                .map(ProtoValueDeserializer::deserialize)
                .collect(Collectors.toCollection(ArrayList::new));
        return TypedValue.fromUdtPrototype(new UDTPrototype(typeName, values));
    }

}

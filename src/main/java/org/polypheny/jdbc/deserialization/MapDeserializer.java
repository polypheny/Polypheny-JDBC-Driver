package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.polypheny.db.protointerface.proto.ProtoEntry;
import org.polypheny.db.protointerface.proto.ProtoMap;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class MapDeserializer implements ValueDeserializer {


    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.OTHER:
                return deserializeAsUdtPrototype(value.getMap(), value.getType().name());
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto map." );
    }

    private TypedValue deserializeAsUdtPrototype(ProtoMap map, String typeName) {
        ArrayList<TypedValue> values = map.getEntriesList().stream()
                .map(this::splitMapEntry)
                .flatMap(List::stream)
                .collect(Collectors.toCollection(ArrayList::new));
        return TypedValue.fromUdtPrototype(new UDTPrototype(typeName, values));
    }

    private List<TypedValue> splitMapEntry(ProtoEntry entry) {
        List<TypedValue> values = new LinkedList<>();
        values.add(ProtoValueDeserializer.deserializeToTypedValue(entry.getKey()));
        values.add(ProtoValueDeserializer.deserializeToTypedValue(entry.getValue()));
        return values;
    }

}

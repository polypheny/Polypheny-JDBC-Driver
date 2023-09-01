package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.polypheny.jdbc.proto.ProtoDocument;
import org.polypheny.jdbc.proto.ProtoEntry;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class DocumentDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.STRUCT:
                return deserializeAsUdtPrototype(value.getDocument(), value.getType().name());
            //TODO implementation
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto document." );
    }

    private TypedValue deserializeAsUdtPrototype(ProtoDocument document, String typeName) {
            ArrayList<TypedValue> values = document.getEntriesList().stream()
                    .map(this::splitDocumentEntry)
                    .flatMap(List::stream)
                    .collect(Collectors.toCollection(ArrayList::new));
            return TypedValue.fromUdtPrototype(new UDTPrototype(typeName, values));
    }

    private List<TypedValue> splitDocumentEntry(ProtoEntry entry) {
        List<TypedValue> values = new LinkedList<>();
        values.add(ProtoValueDeserializer.deserializeToTypedValue(entry.getKey()));
        values.add(ProtoValueDeserializer.deserializeToTypedValue(entry.getValue()));
        return values;
    }

}

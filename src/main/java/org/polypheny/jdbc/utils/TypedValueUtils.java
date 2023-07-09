package org.polypheny.jdbc.utils;

import static java.util.stream.Collectors.toCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.deserialization.ProtoValueDeserializer;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.deserialization.ProtoToJdbcTypeMap;
import org.polypheny.jdbc.types.ProtoToPolyTypeNameMap;
import org.polypheny.jdbc.types.TypedValue;

public class TypedValueUtils {

    public static List<ArrayList<TypedValue>> buildRows( List<Row> rows ) {
        return rows.stream()
                .map( TypedValueUtils::buildRow )
                .collect( Collectors.toList() );
    }


    public static ArrayList<TypedValue> buildRow( Row row ) {
        return row.getValuesList().stream()
                .map( ProtoValueDeserializer::deserialize )
                .collect( toCollection( ArrayList::new ) );
    }

    public static ArrayList<Integer> getTypes(List<TypedValue> typedValues) {
        return typedValues.stream().map( TypedValue::getJdbcType ).collect(toCollection(ArrayList::new));
    }

    public static int getJdbcTypeFromPolyTypeName(String polyTypeName) {
        return ProtoToJdbcTypeMap.getJdbcTypeFromProto( ProtoToPolyTypeNameMap.getProtoTypeFromPolyTypeName( polyTypeName ) );
    }
}

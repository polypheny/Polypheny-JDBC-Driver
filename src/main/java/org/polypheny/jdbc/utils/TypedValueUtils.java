package org.polypheny.jdbc.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.proto.Row;
import org.polypheny.jdbc.types.TypedValue;

public class TypedValueUtils {

    public static List<ArrayList<TypedValue>> buildRows( List<Row> rows ) {
        return rows.stream()
                .map( TypedValueUtils::buildRow )
                .collect( Collectors.toList() );
    }


    public static ArrayList<TypedValue> buildRow( Row row ) {
        return row.getValuesList().stream()
                .map( TypedValue::new )
                .collect( Collectors.toCollection( ArrayList::new ) );
    }

}

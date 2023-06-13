package org.polypheny.jdbc.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyphenyColumnMeta;
import org.polypheny.jdbc.proto.ColumnMeta;

public class ColumnMetaUtils {

    public static ArrayList<PolyphenyColumnMeta> buildColumnMetas( List<ColumnMeta> protoColumnMetas ) {
        return protoColumnMetas.stream().map( PolyphenyColumnMeta::new ).collect( Collectors.toCollection( ArrayList::new ) );
    }

}

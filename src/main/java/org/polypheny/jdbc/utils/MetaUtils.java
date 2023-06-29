package org.polypheny.jdbc.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.jdbc.PolyphenyColumnMeta;
import org.polypheny.jdbc.PolyphenyParameterMeta;
import org.polypheny.jdbc.proto.ColumnMeta;
import org.polypheny.jdbc.proto.ParameterMeta;

public class MetaUtils {

    public static ArrayList<PolyphenyColumnMeta> buildColumnMetas( List<ColumnMeta> protoColumnMetas ) {
        return protoColumnMetas.stream().map( PolyphenyColumnMeta::new ).collect( Collectors.toCollection( ArrayList::new ) );
    }

    public static ArrayList<PolyphenyParameterMeta> buildParameterMetas(List<ParameterMeta> protoParameterMetas) {
        return protoParameterMetas.stream().map( PolyphenyParameterMeta::new ).collect( Collectors.toCollection( ArrayList::new ) );
    }

}

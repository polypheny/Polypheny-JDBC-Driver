package org.polypheny.jdbc.meta;

import org.polypheny.db.protointerface.proto.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MetaUtils {

    public static ArrayList<PolyphenyColumnMeta> buildColumnMetas( List<ColumnMeta> protoColumnMetas ) {
        return protoColumnMetas.stream().map( PolyphenyColumnMeta::new ).collect( Collectors.toCollection( ArrayList::new ) );
    }


    public static ArrayList<PolyphenyParameterMeta> buildParameterMetas( List<ParameterMeta> protoParameterMetas ) {
        return protoParameterMetas.stream().map( PolyphenyParameterMeta::new ).collect( Collectors.toCollection( ArrayList::new ) );
    }


    public enum NamespaceTypes {
        RELATIONAL,
        GRAPH,
        DOCUMENT
    }


    public static String convertToRegex( String jdbcPattern ) {
        return jdbcPattern.replace( "_", "(.)" ).replace( "%", "(.*)" );
    }

}

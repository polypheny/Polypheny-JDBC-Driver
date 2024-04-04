package org.polypheny.jdbc.meta;

import org.polypheny.db.protointerface.proto.*;

import java.util.List;
import java.util.stream.Collectors;

public class MetaUtils {

    public static List<PolyphenyColumnMeta> buildColumnMetas( List<ColumnMeta> protoColumnMetas ) {
        return protoColumnMetas.stream().map( PolyphenyColumnMeta::new ).collect( Collectors.toList() );
    }


    public static List<PolyphenyParameterMeta> buildParameterMetas( List<ParameterMeta> protoParameterMetas ) {
        return protoParameterMetas.stream().map( PolyphenyParameterMeta::new ).collect( Collectors.toList() );
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

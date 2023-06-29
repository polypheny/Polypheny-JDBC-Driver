package org.polypheny.jdbc.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.jdbc.types.TypedValue;

public class NamedParameterUtils {

    public static Map<String, TypedValue> convertToParameterMap( List<TypedValue> parameters ) {
        AtomicInteger parameterIndex = new AtomicInteger();
        Map<String, TypedValue> parameterMap = new HashMap<>();
        parameters.forEach( p -> parameterMap.put( String.valueOf( parameterIndex.getAndIncrement() ), p ) );
        return parameterMap;
    }

}
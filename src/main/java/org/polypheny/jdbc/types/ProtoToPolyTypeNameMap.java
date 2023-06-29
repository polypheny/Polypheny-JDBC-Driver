package org.polypheny.jdbc.types;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.ProtoValueType;

public class ProtoToPolyTypeNameMap {

    private static final String COMMON_NAME_PREFIX = "PROTO_VALUE_TYPE_";
    private static final Map<ProtoValueType, String> PROTO_TYPE_TO_POLY_TYPE;


    static {
        PROTO_TYPE_TO_POLY_TYPE = Arrays.stream( ProtoValueType.values() )
                .sequential()
                .collect( ImmutableMap.toImmutableMap( n -> n, n -> removeCommonName( n.name() ) ) );
    }


    private static String removeCommonName( String valueName ) {
        if ( valueName != null && valueName.startsWith( COMMON_NAME_PREFIX ) ) {
            return valueName.substring( COMMON_NAME_PREFIX.length() );
        }
        return valueName;
    }


    public static String getPolyTypeNameFromProto( ProtoValueType protoValueType ) {
        String polyTypeName = PROTO_TYPE_TO_POLY_TYPE.get( protoValueType );
        if ( polyTypeName == null ) {
            throw new IllegalArgumentException( "Invalid proto value type." );
        }
        return polyTypeName;
    }


    public static ProtoValueType getProtoTypeFromPolyTypeName( String polyTypeName ) {
        String protoTypeName = COMMON_NAME_PREFIX + polyTypeName;
        try {
            return ProtoValueType.valueOf( protoTypeName );
        } catch ( IllegalArgumentException e ) {
            throw new ProtoInterfaceServiceException( "Unknown parameter type " + polyTypeName );
        }
    }

}

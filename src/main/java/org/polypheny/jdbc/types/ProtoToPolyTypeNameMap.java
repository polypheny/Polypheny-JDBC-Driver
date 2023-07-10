package org.polypheny.jdbc.types;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.proto.ProtoValue;

public class ProtoToPolyTypeNameMap {

    private static final Map<ProtoValue.ProtoValueType, String> PROTO_TYPE_TO_POLY_TYPE;


    static {
        PROTO_TYPE_TO_POLY_TYPE = Arrays.stream( ProtoValue.ProtoValueType.values() )
                .sequential()
                .collect( ImmutableMap.toImmutableMap( n -> n, n -> n.name() ) );
    }


    public static String getPolyTypeNameFromProto( ProtoValue.ProtoValueType protoValueType ) {
        String polyTypeName = PROTO_TYPE_TO_POLY_TYPE.get( protoValueType );
        if ( polyTypeName == null ) {
            throw new IllegalArgumentException( "Invalid proto value type." );
        }
        return polyTypeName;
    }


    public static ProtoValue.ProtoValueType getProtoTypeFromPolyTypeName( String polyTypeName ) {
        try {
            return ProtoValue.ProtoValueType.valueOf( polyTypeName );
        } catch ( IllegalArgumentException e ) {
            throw new ProtoInterfaceServiceException( "Unknown parameter type " + polyTypeName );
        }
    }

}

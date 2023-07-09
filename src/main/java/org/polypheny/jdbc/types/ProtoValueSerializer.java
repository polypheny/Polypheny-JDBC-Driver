package org.polypheny.jdbc.types;

import com.google.protobuf.MessageLite.Builder;
import java.util.List;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValue;

public class ProtoValueSerializer {

    public static Map<String, ProtoValue> serializeValuesMap( Map<String, TypedValue> parameters ) {
        //TODO implement this
        return null;
    }


    public static List<ProtoValue> serializeValuesList( List<TypedValue> values ) {
        //TODO implement ths
        return null;
    }

}

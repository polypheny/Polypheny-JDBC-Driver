package org.polypheny.jdbc.types;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;

public class ProtoValueDeserializer {

    private static final Map<ProtoValue.ValueCase, ValueDeserializer> VALUE_DESERIALIZERS =
            ImmutableMap.<ProtoValue.ValueCase, ValueDeserializer>builder()
                    .put( ValueCase.BOOLEAN, new BooleanDeserializer() )
                    .put( ValueCase.INTEGER, new IntegerDeserializer() )
                    .put( ValueCase.LONG, new LongDeserializer() )
                    .put( ValueCase.BINARY, new BinaryDeserializer() )
                    .put( ValueCase.DATE, new DateDeserializer() )
                    .put( ValueCase.DOUBLE, new DoubleDeserializer() )
                    .put( ValueCase.FLOAT, new FloatDeserializer() )
                    .put( ValueCase.NULL, new NullDeserializer() )
                    .put( ValueCase.STRING, new StringDeserializer() )
                    .put( ValueCase.TIME, new TimeDeserializer() )
                    .put( ValueCase.TIME_STAMP, new TimeStampDeserializer() )
                    .put( ValueCase.BIG_DECIMAL, new BigDecimalDeserializer() )
                    .build();


    public static Object deserialize( ProtoValue value ) {
        return VALUE_DESERIALIZERS.get( value.getValueCase() ).deserialize( value );
    }
    //TODO TH: fix type in deser.

}

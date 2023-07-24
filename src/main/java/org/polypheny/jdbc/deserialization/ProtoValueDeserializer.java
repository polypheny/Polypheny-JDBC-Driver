package org.polypheny.jdbc.deserialization;


import com.google.common.collect.ImmutableMap;
import java.sql.SQLException;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ValueCase;
import org.polypheny.jdbc.types.TypedValue;

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
                    .put( ValueCase.INTERVAL, new IntervalDeserializer() )
                    .put( ValueCase.USER_DEFINED_TYPE, new UserDefinedTypeDeserializer() )
                    .put( ValueCase.LIST, new ListDeserializer() )
                    .put( ValueCase.MAP, new MapDeserializer() )
                    .put( ValueCase.DOCUMENT, new DocumentDeserializer() )
                    .put( ValueCase.NODE, new NodeDeserializer() )
                    .put( ValueCase.EDGE, new EdgeDeserializer() )
                    .put( ValueCase.PATH, new PathDeserializer() )
                    .put( ValueCase.GRAPH, new GraphDeserializer() )
                    .put( ValueCase.ARRAY, new ArrayDeserializer() )
                    .put( ValueCase.ROWID, new RowIfDeserializer())
                    .build();


    public static TypedValue deserialize( ProtoValue value ) {
        try {
            return VALUE_DESERIALIZERS.get( value.getValueCase() ).deserialize( value );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
    }

}

package org.polypheny.jdbc.types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import org.polypheny.jdbc.proto.ProtoBigDecimal;
import org.polypheny.jdbc.proto.ProtoValue;

public class BigDecimalDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToBigDecimal( value.getBigDecimal() );
    }


    private BigDecimal deserializeToBigDecimal( ProtoBigDecimal protoBigDecimal ) {
        MathContext context = new MathContext( protoBigDecimal.getPrecision() );
        byte[] unscaledValue = protoBigDecimal.getUnscaledValue().toByteArray();
        return new BigDecimal( new BigInteger( unscaledValue ), protoBigDecimal.getScale(), context );
    }

}

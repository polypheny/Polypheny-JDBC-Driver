package org.polypheny.jdbc.deserialization;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoBigDecimal;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class BigDecimalDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.DECIMAL:
                BigDecimal b = deserializeToBigDecimal( value.getBigDecimal() );
                return TypedValue.fromObject( b, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto big decimal." );
    }


    private BigDecimal deserializeToBigDecimal( ProtoBigDecimal protoBigDecimal ) {
        MathContext context = new MathContext( protoBigDecimal.getPrecision() );
        byte[] unscaledValue = protoBigDecimal.getUnscaledValue().toByteArray();
        return new BigDecimal( new BigInteger( unscaledValue ), protoBigDecimal.getScale(), context );
    }

}

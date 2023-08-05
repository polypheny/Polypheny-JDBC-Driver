package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoFloat;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class FloatDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch(jdbcType) {
            case Types.REAL:
                Float f = deserializeToFloat( value.getFloat() );
                return TypedValue.fromObject( f, jdbcType );
            case Types.FLOAT:
                // according to jdbc appendix B.1 floats should internally be represented as doubles
                Double d = deserializeToDouble( value.getFloat() );
                return  TypedValue.fromObject( d, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto float." );
    }


    private Float deserializeToFloat( ProtoFloat protoFloat ) {
        return protoFloat.getFloat();
    }

    private Double deserializeToDouble( ProtoFloat protoFloat ) {
        return (double) protoFloat.getFloat();
    }

}

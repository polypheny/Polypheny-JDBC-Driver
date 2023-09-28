package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.db.protointerface.proto.ProtoInteger;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class IntegerDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.TINYINT:
                Byte b = deserializeToByte( value.getInteger() );
                return TypedValue.fromObject( b, jdbcType );
            case Types.SMALLINT:
                Short s = deserializeToShort( value.getInteger() );
                return TypedValue.fromObject( s, jdbcType );
            case Types.INTEGER:
                Integer i = deserializeToInteger( value.getInteger() );
                return TypedValue.fromObject( i, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto integer." );
    }


    private static Byte deserializeToByte( ProtoInteger protoInteger ) {
        return (byte) protoInteger.getInteger();
    }


    private static Short deserializeToShort( ProtoInteger protoInteger ) {
        return (short) protoInteger.getInteger();
    }


    private static Integer deserializeToInteger( ProtoInteger protoInteger ) {
        return protoInteger.getInteger();
    }

}

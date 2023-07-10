package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.jdbc.proto.ProtoString;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.Type;
import org.polypheny.jdbc.types.TypedValue;

public class StringDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.CHAR:
            case Types.VARCHAR:
                String s = deserilizeToString( value.getString() );
                return TypedValue.fromObject( s, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto string." );
    }


    private String deserilizeToString( ProtoString protoString ) {
        return protoString.getString();
    }

}

package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.db.protointerface.proto.ProtoString;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class StringDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
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

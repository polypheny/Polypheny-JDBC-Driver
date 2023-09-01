package org.polypheny.jdbc.deserialization;

import java.sql.SQLException;
import java.sql.Types;
import java.sql.Date;
import org.polypheny.jdbc.proto.ProtoDate;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class DateDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch(jdbcType) {
            case Types.DATE:
                Date d = deserializeToSqlDate(value.getDate());
                return TypedValue.fromObject( d, ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto date." );
    }


    private Date deserializeToSqlDate( ProtoDate protoDate ) {
        return new Date(protoDate.getDate() * 86400000);
    }

}

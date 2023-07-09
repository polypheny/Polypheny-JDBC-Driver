package org.polypheny.jdbc.deserialization;

import java.sql.Types;
import java.sql.Date;
import org.polypheny.jdbc.proto.ProtoDate;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.types.TypedValue;

public class DateDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserialize( ProtoValue value ) {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch(jdbcType) {
            case Types.DATE:
                Date d = deserializeToSqlDate(value.getDate());
                return TypedValue.fromObject( d, ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() ) );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto date." );
    }


    private Date deserializeToSqlDate( ProtoDate polyDate ) {
        return new Date(polyDate.getDate() );
    }

}

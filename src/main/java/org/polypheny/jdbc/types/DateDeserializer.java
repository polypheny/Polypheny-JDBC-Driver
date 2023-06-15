package org.polypheny.jdbc.types;

import java.sql.Date;
import org.polypheny.jdbc.proto.ProtoDate;
import org.polypheny.jdbc.proto.ProtoValue;

public class DateDeserializer implements ValueDeserializer {

    @Override
    public Object deserialize( ProtoValue value ) {
        return deserializeToSqlSate(value.getDate());
    }


    private Object deserializeToSqlSate( ProtoDate polyDate ) {
        return new Date(polyDate.getDate() );
    }

}

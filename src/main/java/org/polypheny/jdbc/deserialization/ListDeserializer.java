package org.polypheny.jdbc.deserialization;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;

import org.polypheny.db.protointerface.proto.ProtoList;
import org.polypheny.db.protointerface.proto.ProtoValue;
import org.polypheny.db.protointerface.proto.ProtoValue.ValueCase;
import org.polypheny.jdbc.jdbctypes.PolyphenyArray;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class ListDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getValueCase() );
        switch ( jdbcType ) {
            case Types.ARRAY:
                Array a = deserializeToArray( value.getList() );
                return TypedValue.fromObject( a, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto list" );
    }


    private Array deserializeToArray( ProtoList protoList ) throws SQLException {
        ValueCase baseType = protoList.getValues( 0 ).getValueCase();
        List<TypedValue> values = new LinkedList<>();
        for ( ProtoValue protoValue : protoList.getValuesList() ) {
            values.add( ProtoValueDeserializer.deserializeToTypedValue( protoValue ) );
        }
        return new PolyphenyArray( baseType.name(), values );
    }

}

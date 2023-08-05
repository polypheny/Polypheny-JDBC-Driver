package org.polypheny.jdbc.deserialization;

import java.sql.Array;
import java.sql.SQLException;
import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import org.polypheny.jdbc.proto.ProtoArray;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;
import org.polypheny.jdbc.types.PolyphenyArray;
import org.polypheny.jdbc.types.TypedValue;

public class ArrayDeserializer implements ValueDeserializer {

    @Override
    public TypedValue deserializeToTypedValue( ProtoValue value ) throws SQLException {
        int jdbcType = ProtoToJdbcTypeMap.getJdbcTypeFromProto( value.getType() );
        switch ( jdbcType ) {
            case Types.ARRAY:
                Array a = deserializeToArray( value.getArray() );
                return TypedValue.fromObject( a, jdbcType );
        }
        throw new IllegalArgumentException( "Illegal jdbc type for proto integer." );
    }


    private Array deserializeToArray( ProtoArray protoArray ) throws SQLException {
        ProtoValueType baseType = protoArray.getElements( 0 ).getType();
        List<TypedValue> values = new LinkedList<>();
        for ( ProtoValue protoValue : protoArray.getElementsList() ) {
            values.add( ProtoValueDeserializer.deserializeToTypedValue( protoValue ) );
        }
        return new PolyphenyArray( baseType.name(), values );
    }

}

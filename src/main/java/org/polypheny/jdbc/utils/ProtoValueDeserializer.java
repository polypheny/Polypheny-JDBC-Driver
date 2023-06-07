package org.polypheny.jdbc.utils;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import javax.lang.model.type.NullType;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.jdbc.proto.ProtoBinary;
import org.polypheny.jdbc.proto.ProtoBoolean;
import org.polypheny.jdbc.proto.ProtoDate;
import org.polypheny.jdbc.proto.ProtoDouble;
import org.polypheny.jdbc.proto.ProtoFloat;
import org.polypheny.jdbc.proto.ProtoInteger;
import org.polypheny.jdbc.proto.ProtoLong;
import org.polypheny.jdbc.proto.ProtoNull;
import org.polypheny.jdbc.proto.ProtoString;
import org.polypheny.jdbc.proto.ProtoTime;
import org.polypheny.jdbc.proto.ProtoTimeStamp;
import org.polypheny.jdbc.proto.Value;

public class ProtoValueDeserializer {

    public static void deserialize( Value value ) {
        switch(value.getValueCase()) {
            case DATE:
                throw new NotImplementedException("dates can not be deserialized yet");
            case LONG:
                long l = deserialize( value.getLong() );
                System.out.println("Would add \" " + l + "to the result set." );
            case NULL:
            case TIME:
            case FLOAT:
                float f = deserialize( value.getFloat() );
                System.out.println("Would add \" " + f + "to the result set." );
            case BINARY:
            case DOUBLE:
                double d = deserialize( value.getDouble() );
                System.out.println("Would add \" " + d + "to the result set." );
            case STRING:
                String s = deserialize( value.getString() );
                System.out.println("Would add \" " + s + "to the result set." );
            case BOOLEAN:
                boolean b = deserialize( value.getBoolean() );
                System.out.println("Would add \" " + b + "to the result set." );
            case INTEGER:
                int i = deserialize( value.getInteger() );
                System.out.println("Would add \" " + i + "to the result set." );
            case TIME_STAMP:
                throw new NotImplementedException("time stamps can not be deserialized yet");
            case VALUE_NOT_SET:
                throw new IllegalArgumentException("received unknown type from server");
        }
    }

    public static boolean deserialize( ProtoBoolean protoBoolean ) {
        return protoBoolean.getBoolean();
    }


    public static int deserialize( ProtoInteger protoInteger ) {
        return protoInteger.getInteger();
    }


    public static long deserialize( ProtoLong protoLong ) {
        return protoLong.getLong();
    }


    public static byte[] deserialize( ProtoBinary protoBinary ) {
        throw new NotImplementedException("deserialization of binary not implemented yet");
    }


    public static Date deserialize( ProtoDate protoDate ) {
        return new Date(protoDate.getDate());
    }


    public static double deserialize( ProtoDouble protoDouble ) {
        return protoDouble.getDouble();
    }


    public static float deserialize( ProtoFloat protoFloat ) {
    return protoFloat.getFloat();
    }


    public static String deserialize( ProtoString protoString ) {
    return protoString.getString();
    }


    public static Time deserialize( ProtoTime protoTime ) {
        throw new NotImplementedException();
    }


    public static Timestamp deserialize( ProtoTimeStamp protoTimeStamp ) {
        throw new NotImplementedException("deserialization of binary not implemented yet");
    }


    public static NullType deserialize( ProtoNull protoNull ) {
        return null;
    }

}

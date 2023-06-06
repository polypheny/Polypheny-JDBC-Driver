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

public class ProtoValueDeserializer {

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
        //return protoBinary.getBinary().toByteArray();
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

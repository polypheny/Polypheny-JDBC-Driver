package org.polypheny.jdbc.serialisation;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.jdbc.proto.ProtoValue;
import org.polypheny.jdbc.proto.ProtoValue.ProtoValueType;
import org.polypheny.jdbc.types.TypedValue;

public class JdbcToProtoTypeMap {

    private static final Map<Integer, ProtoValue.ProtoValueType> JDBC_TYPE_TO_PROTO =
            ImmutableMap.<Integer, ProtoValue.ProtoValueType>builder()
                    .put( Types.TINYINT, ProtoValueType.INTEGER )
                    .put( Types.SMALLINT, ProtoValueType.INTEGER )
                    .put( Types.INTEGER, ProtoValueType.INTEGER )
                    .put( Types.BIT, ProtoValueType.BOOLEAN )
                    .put( Types.BOOLEAN, ProtoValueType.BOOLEAN )
                    .put( Types.BIGINT, ProtoValueType.BIGINT )
                    .put( Types.FLOAT, ProtoValueType.DOUBLE )
                    .put( Types.REAL, ProtoValueType.DOUBLE )
                    .put( Types.DOUBLE, ProtoValueType.DOUBLE )
                    .put( Types.NUMERIC, ProtoValueType.DECIMAL )
                    .put( Types.DECIMAL, ProtoValueType.DECIMAL )
                    .put( Types.CHAR, ProtoValueType.VARCHAR )
                    .put( Types.VARCHAR, ProtoValueType.VARCHAR )
                    .put( Types.LONGVARCHAR, ProtoValueType.VARCHAR )
                    .put( Types.NCHAR, ProtoValueType.VARCHAR )
                    .put( Types.NVARCHAR, ProtoValueType.VARCHAR )
                    .put( Types.LONGNVARCHAR, ProtoValueType.VARCHAR )
                    .put( Types.DATE, ProtoValueType.DATE )
                    .put( Types.TIME, ProtoValueType.DATE )
                    .put( Types.TIMESTAMP, ProtoValueType.TIMESTAMP )
                    .put( Types.BINARY, ProtoValueType.BINARY )
                    .put( Types.VARBINARY, ProtoValueType.BINARY )
                    .put( Types.LONGVARBINARY, ProtoValueType.BINARY )
                    .put( Types.NULL, ProtoValueType.NULL )
                    .put( Types.OTHER, ProtoValueType.UNSPECIFIED )
                    .put( Types.JAVA_OBJECT, ProtoValueType.UNSPECIFIED )
                    .put( Types.DISTINCT, ProtoValueType.UNSPECIFIED )
                    .put( Types.STRUCT, ProtoValueType.USER_DEFINED_TYPE )
                    .put( Types.ARRAY, ProtoValueType.ARRAY )
                    .put( Types.BLOB, ProtoValueType.BINARY )
                    .put( Types.CLOB, ProtoValueType.BINARY )
                    .put( Types.REF, ProtoValueType.UNSPECIFIED )
                    .put( Types.DATALINK, ProtoValueType.UNSPECIFIED )
                    .put( Types.ROWID, ProtoValueType.UNSPECIFIED )
                    .put( Types.NCLOB, ProtoValueType.BINARY )
                    .put( Types.SQLXML, ProtoValueType.UNSPECIFIED )
                    .put( Types.REF_CURSOR, ProtoValueType.UNSPECIFIED )
                    .put( Types.TIME_WITH_TIMEZONE, ProtoValueType.TIME )
                    .put( Types.TIMESTAMP_WITH_TIMEZONE, ProtoValueType.TIMESTAMP )
                    .build();


    public static ProtoValueType getTypeOf( TypedValue typedValue ) {
        return JDBC_TYPE_TO_PROTO.get( typedValue.getJdbcType() );
    }

}

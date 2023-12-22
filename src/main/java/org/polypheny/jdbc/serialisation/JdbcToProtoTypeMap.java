package org.polypheny.jdbc.serialisation;

import com.google.common.collect.ImmutableMap;
import java.sql.Types;
import java.util.Map;
import org.polypheny.db.protointerface.proto.ProtoPolyType;
import org.polypheny.jdbc.jdbctypes.TypedValue;

public class JdbcToProtoTypeMap {

    private static final Map<Integer, ProtoPolyType> JDBC_TYPE_TO_PROTO =
            ImmutableMap.<Integer, ProtoPolyType>builder()
                    .put( Types.TINYINT, ProtoPolyType.INTEGER )
                    .put( Types.SMALLINT, ProtoPolyType.INTEGER )
                    .put( Types.INTEGER, ProtoPolyType.INTEGER )
                    .put( Types.BIT, ProtoPolyType.BOOLEAN )
                    .put( Types.BOOLEAN, ProtoPolyType.BOOLEAN )
                    .put( Types.BIGINT, ProtoPolyType.BIGINT )
                    .put( Types.FLOAT, ProtoPolyType.DOUBLE )
                    .put( Types.REAL, ProtoPolyType.DOUBLE )
                    .put( Types.DOUBLE, ProtoPolyType.DOUBLE )
                    .put( Types.NUMERIC, ProtoPolyType.DECIMAL )
                    .put( Types.DECIMAL, ProtoPolyType.DECIMAL )
                    .put( Types.CHAR, ProtoPolyType.VARCHAR )
                    .put( Types.VARCHAR, ProtoPolyType.VARCHAR )
                    .put( Types.LONGVARCHAR, ProtoPolyType.VARCHAR )
                    .put( Types.NCHAR, ProtoPolyType.VARCHAR )
                    .put( Types.NVARCHAR, ProtoPolyType.VARCHAR )
                    .put( Types.LONGNVARCHAR, ProtoPolyType.VARCHAR )
                    .put( Types.DATE, ProtoPolyType.DATE )
                    .put( Types.TIME, ProtoPolyType.DATE )
                    .put( Types.TIMESTAMP, ProtoPolyType.TIMESTAMP )
                    .put( Types.BINARY, ProtoPolyType.BINARY )
                    .put( Types.VARBINARY, ProtoPolyType.BINARY )
                    .put( Types.LONGVARBINARY, ProtoPolyType.BINARY )
                    .put( Types.NULL, ProtoPolyType.NULL )
                    .put( Types.OTHER, ProtoPolyType.UNSPECIFIED )
                    .put( Types.JAVA_OBJECT, ProtoPolyType.UNSPECIFIED )
                    .put( Types.DISTINCT, ProtoPolyType.UNSPECIFIED )
                    .put( Types.STRUCT, ProtoPolyType.USER_DEFINED_TYPE )
                    .put( Types.ARRAY, ProtoPolyType.ARRAY )
                    .put( Types.BLOB, ProtoPolyType.BINARY )
                    .put( Types.CLOB, ProtoPolyType.BINARY )
                    .put( Types.REF, ProtoPolyType.UNSPECIFIED )
                    .put( Types.DATALINK, ProtoPolyType.UNSPECIFIED )
                    .put( Types.ROWID, ProtoPolyType.ROW_ID )
                    .put( Types.NCLOB, ProtoPolyType.BINARY )
                    .put( Types.SQLXML, ProtoPolyType.UNSPECIFIED )
                    .put( Types.REF_CURSOR, ProtoPolyType.UNSPECIFIED )
                    .put( Types.TIME_WITH_TIMEZONE, ProtoPolyType.TIME )
                    .put( Types.TIMESTAMP_WITH_TIMEZONE, ProtoPolyType.TIMESTAMP )
                    .build();


    public static ProtoPolyType getTypeOf( TypedValue typedValue ) {
        return JDBC_TYPE_TO_PROTO.get( typedValue.getJdbcType() );
    }

}

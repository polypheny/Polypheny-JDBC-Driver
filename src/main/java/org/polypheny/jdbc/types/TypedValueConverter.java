package org.polypheny.jdbc.types;

import java.sql.Types;

public class TypedValueConverter {
    public Object toObject(TypedValue typedValue) {
        switch ( typedValue.getJdbcType() ) {
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:

            case Types.NUMERIC:
                break;
            case Types.DECIMAL:
                break;
            case    Types.BIT:
                break;
            case Types.BOOLEAN:
                break;
            case Types.TINYINT:
                break;
            case Types.SMALLINT:
                break;
            case Types.INTEGER:
                break;
            case Types.BIGINT:
                break;
            case Types.REAL:
                break;
            case Types.FLOAT:
                break;
            case Types.DOUBLE:
                break;
            case Types.BINARY:
                break;
            case Types.VARBINARY:
                break;
            case Types.LONGVARBINARY:
                break;
            case Types.DATE:
                break;
            case  Types.TIME:
                break;
            case Types.TIMESTAMP:
                break;
            case Types.DISTINCT:
                break;
            case Types.CLOB:
                break;
            case Types.BLOB:
                break;
            case Types.ARRAY:
                break;
            case Types.STRUCT:
                break;
            case Types.REF:
                break;
            case  Types.DATALINK:
                break;
            case Types.JAVA_OBJECT:
                break;
            case Types.ROWID:
                break;
            case Types.NCHAR:
                break;
            case Types.NVARCHAR:
                break;
            case  Types.LONGNVARCHAR:
                break;
            case Types.NCLOB:
                break;
            case Types.SQLXML:
                break;
        }
        throw new IllegalArgumentException("No conversion to object possible for jdbc type: " + typedValue.getJdbcType());
    }



}

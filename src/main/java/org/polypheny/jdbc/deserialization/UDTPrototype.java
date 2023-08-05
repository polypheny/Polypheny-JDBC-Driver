package org.polypheny.jdbc.deserialization;

import lombok.Getter;
import org.polypheny.jdbc.ProtoInterfaceServiceException;
import org.polypheny.jdbc.ProtoInterfaceErrors;
import org.polypheny.jdbc.types.TypedValue;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;

public class UDTPrototype implements SQLInput {
    @Getter
    private final String typeName;
    private final ArrayList<TypedValue> values;
    private int currentIndex;
    private boolean lastValueWasNull;
    private boolean isFinalized;

    public UDTPrototype(String typeName, ArrayList<TypedValue> values) {
        this.typeName = typeName;
        this.values = values;
        this.currentIndex = -1;
        this.lastValueWasNull = true;
        this.isFinalized = false;
    }

    public UDTPrototype(String typeName) {
        this.typeName = typeName;
        this.values = new ArrayList<>();
        this.currentIndex = -1;
        this.lastValueWasNull = true;
        this.isFinalized = false;
    }

    public void addValue(TypedValue value) throws SQLException {
        if (isFinalized) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Can't add values to finalized prototype.");
        }
        values.add(value);
    }

    private TypedValue getNextValue() throws SQLException {
        if (!isFinalized) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.OPERATION_ILLEGAL, "Can't read value from unfinalized prototype.");
        }
        currentIndex++;
        if (currentIndex >= values.size()) {
            throw new ProtoInterfaceServiceException( ProtoInterfaceErrors.UDT_REACHED_END, "Reached end of udt value stream.");
        } else {
            TypedValue currentValue = values.get(currentIndex);
            lastValueWasNull = currentValue.isNull();
            return currentValue;
        }
    }

    @Override
    public String readString() throws SQLException {
        return getNextValue().asString();
    }

    @Override
    public boolean readBoolean() throws SQLException {
        return getNextValue().asBoolean();
    }

    @Override
    public byte readByte() throws SQLException {
        return getNextValue().asByte();
    }

    @Override
    public short readShort() throws SQLException {
        return getNextValue().asShort();
    }

    @Override
    public int readInt() throws SQLException {
        return getNextValue().asInt();
    }

    @Override
    public long readLong() throws SQLException {
        return getNextValue().asLong();
    }

    @Override
    public float readFloat() throws SQLException {
        return getNextValue().asFloat();
    }

    @Override
    public double readDouble() throws SQLException {
        return getNextValue().asDouble();
    }

    @Override
    public BigDecimal readBigDecimal() throws SQLException {
        return getNextValue().asBigDecimal();
    }

    @Override
    public byte[] readBytes() throws SQLException {
        return getNextValue().asBytes();
    }

    @Override
    public Date readDate() throws SQLException {
        return getNextValue().asDate();
    }

    @Override
    public Time readTime() throws SQLException {
        return getNextValue().asTime();
    }

    @Override
    public Timestamp readTimestamp() throws SQLException {
        return getNextValue().asTimestamp();
    }

    @Override
    public Reader readCharacterStream() throws SQLException {
        return getNextValue().asCharacterStream();
    }

    @Override
    public InputStream readAsciiStream() throws SQLException {
        return getNextValue().asAsciiStream();
    }

    @Override
    public InputStream readBinaryStream() throws SQLException {
        return getNextValue().asBinaryStream();
    }

    @Override
    public Object readObject() throws SQLException {
        return getNextValue().asObject();
    }

    @Override
    public Ref readRef() throws SQLException {
        return getNextValue().asRef();
    }

    @Override
    public Blob readBlob() throws SQLException {
        return getNextValue().asBlob();
    }

    @Override
    public Clob readClob() throws SQLException {
        return getNextValue().asClob();
    }

    @Override
    public Array readArray() throws SQLException {
        return getNextValue().asArray();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return lastValueWasNull;
    }

    @Override
    public URL readURL() throws SQLException {
        return getNextValue().asUrl();
    }

    @Override
    public NClob readNClob() throws SQLException {
        return getNextValue().asNClob();
    }

    @Override
    public String readNString() throws SQLException {
        return getNextValue().asNString();
    }

    @Override
    public SQLXML readSQLXML() throws SQLException {
        return getNextValue().asSQLXML();
    }

    @Override
    public RowId readRowId() throws SQLException {
        return getNextValue().asRowId();
    }
}

package org.polypheny.jdbc.meta;

public class GenericMetaContainer {
    Object[] values;

    public GenericMetaContainer(Object... values) {
        this.values = values;
    }

    public Object getValue(int valueIndex) {
        return values[valueIndex];
    }
}

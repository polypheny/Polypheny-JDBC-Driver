package org.polypheny.jdbc;

import java.util.HashMap;

public class ModificationAwareHashMap<K, V> extends HashMap<K, V> {
    private HashMap<K,V> lastCheckpoint;

    public ModificationAwareHashMap() {
        this.lastCheckpoint = new HashMap<>();
    }

    public boolean isModified() {
        return this.equals( lastCheckpoint );
    }

    public void setCheckpoint() {
        lastCheckpoint = new HashMap<>(this);
    }
}

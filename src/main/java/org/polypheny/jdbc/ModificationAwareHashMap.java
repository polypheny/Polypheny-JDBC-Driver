package org.polypheny.jdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class ModificationAwareHashMap<K, V> extends HashMap<K, V> {


    private boolean isModified;


    public boolean isModified() {
        return isModified;
    }


    public void resetIsModified() {
        isModified = false;
    }


    public ModificationAwareHashMap() {
        this.isModified = false;
    }


    @Override
    public V put( K key, V value ) {
        isModified = true;
        return super.put( key, value );
    }


    @Override
    public void putAll( Map<? extends K, ? extends V> m ) {
        isModified = true;
        super.putAll( m );
    }


    @Override
    public boolean remove( Object key, Object value ) {
        isModified = true;
        return super.remove( key, value );
    }


    @Override
    public void clear() {
        isModified = true;
        super.clear();
    }


    @Override
    public V putIfAbsent( K key, V value ) {
        isModified = true;
        return super.putIfAbsent( key, value );
    }


    @Override
    public V replace( K key, V value ) {
        isModified = true;
        return super.replace( key, value );
    }


    @Override
    public boolean replace( K key, V oldValue, V newValue ) {
        isModified = true;
        return super.replace( key, oldValue, newValue );
    }


    @Override
    public V computeIfPresent( K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction ) {
        isModified = true;
        return super.computeIfPresent( key, remappingFunction );
    }


    @Override
    public V computeIfAbsent( K key, Function<? super K, ? extends V> mappingFunction ) {
        isModified = true;
        return super.computeIfAbsent( key, mappingFunction );
    }


    @Override
    public V compute( K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction ) {
        isModified = true;
        return super.compute( key, remappingFunction );
    }


    @Override
    public V merge( K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction ) {
        isModified = true;
        return super.merge( key, value, remappingFunction );
    }


    @Override
    public void replaceAll( BiFunction<? super K, ? super V, ? extends V> function ) {
        isModified = true;
        super.replaceAll( function );
    }

}

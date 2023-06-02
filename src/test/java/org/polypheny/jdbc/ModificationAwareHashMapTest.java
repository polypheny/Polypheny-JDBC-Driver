package org.polypheny.jdbc;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ModificationAwareHashMapTest {

    ModificationAwareHashMap<String, String> modificationAwareHashMap;


    @BeforeClass
    public static void setUpClass() {
    }


    @AfterClass
    public static void tearDownClass() {
    }


    @Before
    public void setUp() {
        modificationAwareHashMap = new ModificationAwareHashMap<>();
    }


    @After
    public void tearDown() {
    }


    @Test
    public void init__notModified() {
        assertFalse( modificationAwareHashMap.isModified() );
    }


    @Test
    public void setCheckpoint__notModified() {
        put_String_String__modified();
        modificationAwareHashMap.setCheckpoint();
        assertFalse( modificationAwareHashMap.isModified() );
    }

    @Test
    public void revertedChange__notModified() {
        modificationAwareHashMap.put( "k1", "v1" );
        modificationAwareHashMap.remove( "k1");
    }


    @Test
    public void put_String_String__modified() {
        modificationAwareHashMap.put( "k1", "v1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void putAll_StringMap__modified() {
        Map<String, String> input = new HashMap<>();
        input.put( "k1", "v1" );
        input.put( "k2", "v2" );
        modificationAwareHashMap.putAll( input );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void remove_String_String__modified() {
        Map<String, String> input = new HashMap<>();
        input.put( "k1", "v1" );
        input.put( "k2", "v2" );
        modificationAwareHashMap.putAll( input );
        modificationAwareHashMap.remove( "k1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void clear_String_String__modified() {
        modificationAwareHashMap.put( "k1", "v1" );
        modificationAwareHashMap.setCheckpoint();
        modificationAwareHashMap.clear();
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void putIfAbsent_String_String__modified() {
        modificationAwareHashMap.putIfAbsent( "k1", "v1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void replace_String_String__modified() {
        modificationAwareHashMap.put( "k1", "v1" );
        modificationAwareHashMap.setCheckpoint();
        modificationAwareHashMap.replace( "k1", "newV1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void replace_String_String_String__modified() {
        modificationAwareHashMap.put( "k1", "oldV1" );
        modificationAwareHashMap.setCheckpoint();
        modificationAwareHashMap.replace( "k1", "oldV1", "newV1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void computeIfAbsent_String__modified() {
        modificationAwareHashMap.computeIfAbsent( "k1", (k -> "computedV1") );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void computeIfPresent_String__modified() {
        modificationAwareHashMap.put( "k1", "v1" );
        modificationAwareHashMap.computeIfPresent( "k1", ( k, v ) -> "computedV1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void compute_String__modified() {
        modificationAwareHashMap.put( "k1", "v1" );
        modificationAwareHashMap.compute( "k1", ( k, v ) -> "computedV1" );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void merge_String__modified() {
        modificationAwareHashMap.put( "k1", "map1V1" );
        modificationAwareHashMap.put( "k2", "map1V2" );
        Map<String, String> otherMap = new HashMap<>();
        otherMap.put( "k1", "map2V1" );
        otherMap.put( "k2", "map2V2" );
        otherMap.forEach( ( k, v ) -> modificationAwareHashMap.merge( k, v, ( v1, v2 ) -> v1.equals( v2 ) ? v1 : v1 + v2 ) );
        assertTrue( modificationAwareHashMap.isModified() );
    }


    @Test
    public void replaceAll_String__modified() {
        modificationAwareHashMap.put( "k1", "map1V1" );
        modificationAwareHashMap.replaceAll( ( k, oldV ) -> "replaced" + oldV );
        assertTrue( modificationAwareHashMap.isModified() );
    }

}

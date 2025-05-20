package com.dynamolite;

import org.junit.Test;
import static org.junit.Assert.*;

public class DynamoLiteTest {
    @Test
    public void testVersionVector() {
        VersionVector v1 = new VersionVector("node1");
        VersionVector v2 = new VersionVector("node2");

        v1.increment();
        v2.increment();
        v2.increment();

        assertEquals(1, v1.compare(v2));
        assertEquals(-1, v2.compare(v1));
    }

    @Test
    public void testConsistentHashRing() {
        ConsistentHashRing ring = new ConsistentHashRing();
        ring.addNode("node1");
        ring.addNode("node2");
        ring.addNode("node3");

        String key = "testKey";
        String node = ring.getNode(key);
        assertNotNull(node);
        assertTrue(ring.getAllNodes().contains(node));
    }

    @Test
    public void testStorage() {
        Storage storage = new Storage("test_data");
        VersionVector version = new VersionVector("node1");
        version.increment();

        storage.put("key1", "value1", version);
        Storage.Value value = storage.get("key1");

        assertNotNull(value);
        assertEquals("value1", value.getData());
        assertEquals(version, value.getVersion());
    }
} 
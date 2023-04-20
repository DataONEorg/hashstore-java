package org.dataone.hashstore.hashfs;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

/**
 * Test class for HashAddress
 */
public class HashAddressTest {
    public String id = "";
    public String relpath = "";
    public String abspath = "";
    public boolean isDuplicate;
    public Map<String, String> hexDigests;

    /**
     * Initialize HashAddress variables for test efficiency purposes
     */
    @Before
    public void initializeInstanceVariables() {
        this.id = "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        this.relpath = "/rel/test/directory";
        this.abspath = "/abs/test/directory";
        this.isDuplicate = true;
        this.hexDigests = new HashMap<>();
        this.hexDigests.put("md5", "f4ea2d07db950873462a064937197b0f");
        this.hexDigests.put("sha1", "3d25436c4490b08a2646e283dada5c60e5c0539d");
        this.hexDigests.put("sha256", "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        this.hexDigests.put("sha384",
                "a204678330fcdc04980c9327d4e5daf01ab7541e8a351d49a7e9c5005439dce749ada39c4c35f573dd7d307cca11bea8");
        this.hexDigests.put("sha512",
                "bf9e7f4d4e66bd082817d87659d1d57c2220c376cd032ed97cadd481cf40d78dd479cbed14d34d98bae8cebc603b40c633d088751f07155a94468aa59e2ad109");
    }

    /**
     * Check HashAddress constructor
     */
    @Test
    public void testHashAddress() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        assertNotNull(hashad);
    }

    /**
     * Check HashAddress get id
     */
    @Test
    public void testHashAddressGetId() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        String hashad_id = hashad.getId();
        assertEquals(hashad_id, id);
    }

    /**
     * Check HashAddress get rel path
     */
    @Test
    public void testHashAddressGetRelPath() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        String hashad_relpath = hashad.getRelPath();
        assertEquals(hashad_relpath, relpath);
    }

    /**
     * Check HashAddress get abs path
     */
    @Test
    public void testHashAddressGetAbsPath() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        String hashad_abspath = hashad.getAbsPath();
        assertEquals(hashad_abspath, abspath);
    }

    /**
     * Check HashAddress get isDuplicate
     */
    @Test
    public void testHashAddressGetIsDuplicate() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        boolean hashad_isDuplicate = hashad.getIsDuplicate();
        assertEquals(hashad_isDuplicate, isDuplicate);
    }

    /**
     * Check HashAddress get hexDigests
     */
    @Test
    public void testHashAddressGetHexDigests() {
        HashAddress hashad = new HashAddress(this.id, this.relpath, this.abspath, this.isDuplicate, this.hexDigests);
        Map<String, String> hashad_map = hashad.getHexDigests();
        assertEquals(hashad_map, hexDigests);
    }
}

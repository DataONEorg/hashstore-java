package org.dataone.hashstore;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import org.junit.Test;

import org.dataone.hashstore.hashfs.HashAddress;

/**
 * Test class for HashAddres
 */
public class HashAddressTest {

    /**
     * Check HashAddress constructor and get methods
     */
    @Test
    public void checkGetMethods() {
        String id = "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        String relpath = "/rel/test/directory";
        String abspath = "/abs/test/directory";
        boolean isDuplicate = true;
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("md5", "f4ea2d07db950873462a064937197b0f");
        hexDigests.put("sha1", "3d25436c4490b08a2646e283dada5c60e5c0539d");
        hexDigests.put("sha256", "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        hexDigests.put("sha384",
                "a204678330fcdc04980c9327d4e5daf01ab7541e8a351d49a7e9c5005439dce749ada39c4c35f573dd7d307cca11bea8");
        hexDigests.put("sha512",
                "bf9e7f4d4e66bd082817d87659d1d57c2220c376cd032ed97cadd481cf40d78dd479cbed14d34d98bae8cebc603b40c633d088751f07155a94468aa59e2ad109");

        HashAddress hashad = new HashAddress(id, relpath, abspath, isDuplicate, hexDigests);

        assertNotNull(hashad);

        String hashad_id = hashad.getId();
        String hashad_relpath = hashad.getRelPath();
        String hashad_abspath = hashad.getAbsPath();
        boolean hashad_isDuplicate = hashad.getIsDuplicate();
        Map<String, String> hashad_map = hashad.getHexDigests();

        assertEquals(hashad_id, id);
        assertEquals(hashad_relpath, relpath);
        assertEquals(hashad_abspath, abspath);
        assertEquals(hashad_isDuplicate, isDuplicate);
        assertEquals(hashad_map, hexDigests);
    }
}

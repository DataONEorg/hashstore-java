package org.dataone.hashstore;

import static org.junit.Assert.assertTrue;

import org.dataone.hashstore.filehashstore.FileHashStore;
import org.junit.Test;

/**
 * Test class for HashStoreFactory
 */
public class HashStoreTest {
    public static HashStoreFactory hashStore = new HashStoreFactory();

    /**
     * Check object store directory are created after initialization
     */
    @Test
    public void getHashStore() throws Exception {
        HashStore mystore = HashStoreFactory.getHashStore("filehashstore");
        assertTrue(mystore instanceof FileHashStore);
    }
}

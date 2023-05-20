package org.dataone.hashstore;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for HashStoreFactory
 */
public class HashStoreTest {
    public static HashStoreFactory hashStore = new HashStoreFactory();
    private static HashStore mystore;

    @BeforeClass
    public static void getHashStore() {
        try {
            mystore = HashStoreFactory.getHashStore("filehashstore");
        } catch (IOException ioe) {
            fail("IOException encountered: " + ioe.getMessage());
        }
    }

    /**
     * Check that mystore is an instance of "filehashstore"
     */
    @Test
    public void isHashStore() {
        assertNotNull(mystore);
        assertTrue(mystore instanceof FileHashStore);
    }

    /**
     * Delete tmp folder that HashStore created in "/tmp/filehashstore"
     */
    @AfterClass
    public static void deleteHashStore() {
        try {
            String tmpHashstoreFolderPath = "/tmp/filehashstore"; // Path set in properties file
            Path folder = Paths.get(tmpHashstoreFolderPath);
            Files.walk(folder)
                    .sorted(java.util.Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(java.io.File::delete);
            System.out.println("Folder deleted successfully: " + folder);
        } catch (HashStoreFactoryException hsfe) {
            fail("HashStoreFactoryException encountered: " + hsfe.getMessage());
        } catch (IOException ioe) {
            fail("IOException encountered: " + ioe.getMessage());
        }
    }
}

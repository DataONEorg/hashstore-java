package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class for HashStoreFactory
 */
public class HashStoreTest {
    private static HashStore mystore;
    private static final TestDataHarness testData = new TestDataHarness();

    @BeforeClass
    public static void getHashStore() {
        try {
            mystore = HashStoreFactory.getHashStore("filehashstore");
        } catch (Exception e) {
            e.printStackTrace();
            fail("HashStoreTest - Exception encountered: " + e.getMessage());
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
     * Test mystore stores file successfully
     */
    @Test
    public void hashStore_storeObjects() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = mystore.storeObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, objInfo.getId());
            assertTrue(Files.exists(Paths.get(objInfo.getAbsPath())));
        }
    }

    /**
     * Delete tmp folder and files that HashStore created in "/tmp/filehashstore"
     */
    @AfterClass
    public static void deleteHashStore() {
        String tmpHashstoreFolderPath = "/tmp/filehashstore"; // Path set in properties file
        Path folder = Paths.get(tmpHashstoreFolderPath);
        try (Stream<Path> pathStream = Files.walk(folder)) {
            pathStream.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            System.out.println("Folder deleted successfully: " + folder);
        } catch (HashStoreFactoryException hsfe) {
            fail("HashStoreTest - HashStoreFactoryException encountered: " + hsfe.getMessage());
        } catch (IOException ioe) {
            fail("HashStoreTest - deleteHashStore: IOException encountered: " + ioe.getMessage());
        }
    }
}

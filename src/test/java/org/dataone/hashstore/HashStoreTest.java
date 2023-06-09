package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for HashStoreFactory
 */
public class HashStoreTest {
    private static HashStore hashStore;
    private static final TestDataHarness testData = new TestDataHarness();

    @Before
    public void getHashStore() {
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        Path rootDirectory = this.tempFolder.getRoot().toPath().resolve("metacat");

        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");

        try {
            hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);
        } catch (Exception e) {
            e.printStackTrace();
            fail("HashStoreTest - Exception encountered: " + e.getMessage());
        }
    }

    /**
     * Temporary folder for tests to run in
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check that mystore is an instance of "filehashstore"
     */
    @Test
    public void isHashStore() {
        assertNotNull(hashStore);
        assertTrue(hashStore instanceof FileHashStore);
    }

    /**
     * Check that getHashStore throws exception when classPackage is null
     */
    @Test(expected = HashStoreFactoryException.class)
    public void hashStore_nullClassPackage() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", "/test");
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        hashStore = HashStoreFactory.getHashStore(null, storeProperties);
    }

    /**
     * Check that getHashStore throws exception when classPackage is null
     */
    @Test(expected = HashStoreFactoryException.class)
    public void hashStore_nullStoreProperties() throws Exception {
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        hashStore = HashStoreFactory.getHashStore(classPackage, null);
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
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, objInfo.getId());
            assertTrue(Files.exists(Paths.get(objInfo.getAbsPath())));
        }
    }
}

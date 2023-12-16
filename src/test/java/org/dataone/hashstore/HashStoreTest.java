package org.dataone.hashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for HashStoreFactory
 */
public class HashStoreTest {
    private static HashStore hashStore;
    private static final TestDataHarness testData = new TestDataHarness();

    @BeforeEach
    public void getHashStore() {
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        Path rootDirectory = tempFolder.resolve("metacat");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0"
        );

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
    @TempDir
    public Path tempFolder;


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
    @Test
    public void hashStore_classPackageNull() {
        assertThrows(HashStoreFactoryException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", "/test");
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0"
            );

            hashStore = HashStoreFactory.getHashStore(null, storeProperties);
        });
    }

    /**
     * Check that getHashStore throws exception when classPackage is not found
     */
    @Test
    public void hashStore_classPackageNotFound() {
        assertThrows(HashStoreFactoryException.class, () -> {
            String classPackage = "org.dataone.hashstore.filehashstore.AnotherHashStore";

            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", "/test");
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0"
            );

            hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);

        });
    }

    /**
     * Check that getHashStore throws exception when storeProperties is null
     */
    @Test
    public void hashStore_nullStoreProperties() {
        assertThrows(HashStoreFactoryException.class, () -> {
            String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
            hashStore = HashStoreFactory.getHashStore(classPackage, null);
        });
    }

    /**
     * Test hashStore instance stores file successfully
     */
    @Test
    public void hashStore_storeObjects() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo objInfo = hashStore.storeObject(dataStream, pid, null, null, null, -1);

            // Check id (sha-256 hex digest of the ab_id, aka object_cid)
            String objContentId = testData.pidData.get(pid).get("sha256");
            assertEquals(objContentId, objInfo.getId());
        }
    }
}

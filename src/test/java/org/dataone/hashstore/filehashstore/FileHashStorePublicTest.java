package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for FileHashStore constructor
 */
public class FileHashStorePublicTest {
    private static Path rootDirectory;
    private static Path objStringFull;
    private static Path objTmpStringFull;
    private static FileHashStore fileHashStore;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize FileHashStore
     */
    @BeforeClass
    public static void initializeFileHashStore() {
        Path root = tempFolder.getRoot().toPath();
        rootDirectory = root.resolve("metacat");
        objStringFull = rootDirectory.resolve("objects");
        objTmpStringFull = rootDirectory.resolve("objects/tmp");

        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");

        try {
            fileHashStore = new FileHashStore(storeProperties);
        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException encountered: " + e.getMessage());
        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
    }

    /**
     * Temporary folder for tests to run in
     */
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check object store directory are created after initialization
     */
    @Test
    public void initObjDirectory() {
        Path checkStorePath = objStringFull;
        assertTrue(Files.exists(checkStorePath));
    }

    /**
     * Check object store tmp directory are created after initialization
     */
    @Test
    public void initObjTmpDirectory() {
        Path checkTmpPath = objTmpStringFull;
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Test invalid depth value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalDepthArg() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", Paths.get("/test/path"));
        storeProperties.put("storeDepth", 0);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Test invalid width value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalWidthArg() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", Paths.get("/test/path"));
        storeProperties.put("storeDepth", 2);
        storeProperties.put("storeWidth", 0);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Test unsupported algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalAlgorithmArg() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", Paths.get("/test/path"));
        storeProperties.put("storeDepth", 2);
        storeProperties.put("storeWidth", 0);
        storeProperties.put("storeAlgorithm", "SM2");
        new FileHashStore(storeProperties);
    }

    /**
     * Confirm that exception is thrown when storeDirectory is null
     */
    @Test(expected = NullPointerException.class)
    public void initDefaultStore_directoryNull() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", null);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Check that a hashstore configuration file is written and exists
     */
    @Test
    public void testPutHashStoreYaml() {
        Path hashStoreYamlFilePath = Paths.get(rootDirectory + "/hashstore.yaml");
        assertTrue(Files.exists(hashStoreYamlFilePath));
    }

    /**
     * Confirm retrieved 'hashstore.yaml' file content is accurate
     */
    @Test
    public void testGetHashStoreYaml() {
        HashMap<String, Object> hsProperties = fileHashStore.getHashStoreYaml(rootDirectory);
        assertEquals(hsProperties.get("storePath"), rootDirectory);
        assertEquals(hsProperties.get("storeDepth"), 3);
        assertEquals(hsProperties.get("storeWidth"), 2);
        assertEquals(hsProperties.get("storeAlgorithm"), "SHA-256");
    }

    /**
     * Test FileHashStore instantiates with matching config
     */
    @Test
    public void testExistingHashStoreConfiguration_sameConfig() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when algorithm is
     * different when instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffAlgorithm() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "MD5");
        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when depth is
     * different when instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffDepth() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 2);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when width is
     * different when instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffWidth() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 1);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);
    }

    /**
     * Check that exception is raised when HashStore present but missing
     * configuration file 'hashstore.yaml'
     *
     * Note, we are checking for an IllegalArgumentException because the try block
     * in the code when walking over files attempts to suppress the thrown
     * 'IllegalStateException' (which won't retain the original exception).
     * Expected exception is verified by asserting 'true' from when it is
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_missingYaml() throws Exception {
        // Create separate store
        HashMap<String, Object> storeProperties = new HashMap<>();
        Path newStoreDirectory = rootDirectory.resolve("test");
        storeProperties.put("storePath", newStoreDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        FileHashStore secondHashStore = new FileHashStore(storeProperties);

        // Confirm config present
        Path newStoreHashStoreYaml = newStoreDirectory.resolve("hashstore.yaml");
        assertTrue(Files.exists(newStoreHashStoreYaml));

        // Store objects
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            secondHashStore.storeObject(dataStream, pid, null, null, null);
        }

        // Delete configuration
        Files.delete(newStoreHashStoreYaml);

        // Instantiate second HashStore
        new FileHashStore(storeProperties);
    }
}

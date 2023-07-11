package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

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
    private static Path metadataStringFull;
    private static Path metadataTmpStringFull;
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
        metadataStringFull = rootDirectory.resolve("metadata");
        metadataTmpStringFull = rootDirectory.resolve("metadata/tmp");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

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
     * Test constructor invalid depth value
     */
    @Test(expected = NullPointerException.class)
    public void constructor_nullProperties() throws Exception {
        new FileHashStore(null);
    }

    /**
     * Test constructor null store path
     */
    @Test(expected = NullPointerException.class)
    public void constructor_nullStorePath() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", null);
        storeProperties.setProperty("storeDepth", "0");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor invalid depth property value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalDepthArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "0");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor invalid width property value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalWidthArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "0");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor unsupported algorithm property value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalAlgorithmArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "MD5");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor empty algorithm property value throws exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_emptyAlgorithmArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor algorithm property value with empty spaces throws exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_emptySpacesAlgorithmArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "       ");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor empty metadata namespace property value throws exception
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_emptyMetadataNameSpaceArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "MD5");
        storeProperties.setProperty("storeMetadataNamespace", "");

        new FileHashStore(storeProperties);
    }

    /**
     * Test constructor metadata namespace property value with empty spaces
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_emptySpacesMetadataNameSpaceArg() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "MD5");
        storeProperties.setProperty("storeMetadataNamespace", "     ");

        new FileHashStore(storeProperties);
    }

    /**
     * Confirm that exception is thrown when storeDirectory property value is null
     */
    @Test(expected = NullPointerException.class)
    public void initDefaultStore_directoryNull() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", null);
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Check object store directory is created after initialization
     */
    @Test
    public void initObjDirectory() {
        Path checkObjectStorePath = objStringFull;
        assertTrue(Files.isDirectory(checkObjectStorePath));
    }

    /**
     * Check object store tmp directory is created after initialization
     */
    @Test
    public void initObjTmpDirectory() {
        Path checkTmpPath = objTmpStringFull;
        assertTrue(Files.isDirectory(checkTmpPath));
    }

    /**
     * Check metadata store directory is created after initialization
     */
    @Test
    public void initMetadataDirectory() {
        Path checkMetadataStorePath = metadataStringFull;
        assertTrue(Files.isDirectory(checkMetadataStorePath));
    }

    /**
     * Check metadata store tmp directory is created after initialization
     */
    @Test
    public void initMetadataTmpDirectory() {
        Path checkMetadataTmpPath = metadataTmpStringFull;
        assertTrue(Files.isDirectory(checkMetadataTmpPath));
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
    public void testGetHashStoreYaml() throws IOException {
        HashMap<String, Object> hsProperties = fileHashStore.getHashStoreYaml(rootDirectory);
        assertEquals(hsProperties.get("storePath"), rootDirectory);
        assertEquals(hsProperties.get("storeDepth"), 3);
        assertEquals(hsProperties.get("storeWidth"), 2);
        assertEquals(hsProperties.get("storeAlgorithm"), "SHA-256");
        assertEquals(hsProperties.get("storeMetadataNamespace"),
                "http://ns.dataone.org/service/types/v2.0");
    }

    /**
     * Test FileHashStore instantiates with matching config
     */
    @Test
    public void testExistingHashStoreConfiguration_sameConfig() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when algorithm is different when
     * instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffAlgorithm() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "MD5");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when depth is different when
     * instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffDepth() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "2");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when width is different when
     * instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffWidth() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "1");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when metadata formatId is different
     * when instantiating FileHashStore
     */
    @Test(expected = IllegalArgumentException.class)
    public void testExistingHashStoreConfiguration_diffMetadataNamespace() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.test.org/service/types/v2.0");

        new FileHashStore(storeProperties);
    }

    /**
     * Check that exception is raised when HashStore present but missing configuration file
     * 'hashstore.yaml'
     */
    @Test(expected = IllegalStateException.class)
    public void testExistingHashStoreConfiguration_missingYaml() throws Exception {
        // Create separate store
        Path newStoreDirectory = rootDirectory.resolve("test");
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", newStoreDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty("storeMetadataNamespace",
                "http://ns.dataone.org/service/types/v2.0");

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

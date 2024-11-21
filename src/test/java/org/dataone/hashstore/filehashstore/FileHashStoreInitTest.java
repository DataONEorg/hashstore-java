package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

import org.dataone.hashstore.HashStore;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for FileHashStore constructor
 */
public class FileHashStoreInitTest {
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
    @BeforeEach
    public void initializeFileHashStore() {
        Path root = tempFolder;
        rootDirectory = root.resolve("hashstore");
        objStringFull = rootDirectory.resolve("objects");
        objTmpStringFull = rootDirectory.resolve("objects/tmp");
        metadataStringFull = rootDirectory.resolve("metadata");
        metadataTmpStringFull = rootDirectory.resolve("metadata/tmp");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

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
    @TempDir
    public Path tempFolder;

    /**
     * Test constructor invalid depth value
     */
    @Test
    public void constructor_nullProperties() {
        assertThrows(IllegalArgumentException.class, () -> new FileHashStore(null));
    }

    /**
     * Test constructor null store path
     */
    @Test
    public void constructor_nullStorePath() {
        assertThrows(NullPointerException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", null);
            storeProperties.setProperty("storeDepth", "0");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor invalid depth property value
     */
    @Test
    public void constructor_illegalDepthArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "0");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor invalid width property value
     */
    @Test
    public void constructor_illegalWidthArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "0");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor unsupported algorithm property value
     */
    @Test
    public void constructor_illegalAlgorithmArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "MD5");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor empty algorithm property value throws exception
     */
    @Test
    public void constructor_emptyAlgorithmArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor algorithm property value with empty spaces throws exception
     */
    @Test
    public void constructor_emptySpacesAlgorithmArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "       ");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor empty metadata namespace property value throws exception
     */
    @Test
    public void constructor_emptyMetadataNameSpaceArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "MD5");
            storeProperties.setProperty("storeMetadataNamespace", "");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test constructor metadata namespace property value with empty spaces
     */
    @Test
    public void constructor_emptySpacesMetadataNameSpaceArg() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "MD5");
            storeProperties.setProperty("storeMetadataNamespace", "     ");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Confirm that exception is thrown when storeDirectory property value is null
     */
    @Test
    public void initDefaultStore_directoryNull() {
        assertThrows(NullPointerException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", null);
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Check object store and tmp directories are created after initialization
     */
    @Test
    public void initObjDirectories() {
        Path checkObjectStorePath = objStringFull;
        assertTrue(Files.isDirectory(checkObjectStorePath));
        Path checkTmpPath = objTmpStringFull;
        assertTrue(Files.isDirectory(checkTmpPath));
    }

    /**
     * Check metadata store and tmp directories are created after initialization
     */
    @Test
    public void initMetadataDirectories() {
        Path checkMetadataStorePath = metadataStringFull;
        assertTrue(Files.isDirectory(checkMetadataStorePath));
        Path checkMetadataTmpPath = metadataTmpStringFull;
        assertTrue(Files.isDirectory(checkMetadataTmpPath));
    }

    /**
     * Check refs tmp, pid and cid directories are created after initialization
     */
    @Test
    public void initRefsDirectories() {
        Path refsPath = rootDirectory.resolve("refs");
        assertTrue(Files.isDirectory(refsPath));
        Path refsTmpPath = rootDirectory.resolve("refs/tmp");
        assertTrue(Files.isDirectory(refsTmpPath));
        Path refsPidPath = rootDirectory.resolve("refs/pids");
        assertTrue(Files.isDirectory(refsPidPath));
        Path refsCidPath = rootDirectory.resolve("refs/cids");
        assertTrue(Files.isDirectory(refsCidPath));
    }

    /**
     * Check that a HashStore configuration file is written and exists
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
        HashMap<String, Object> hsProperties = fileHashStore.loadHashStoreYaml(rootDirectory);
        assertEquals(hsProperties.get("storeDepth"), 3);
        assertEquals(hsProperties.get("storeWidth"), 2);
        assertEquals(hsProperties.get("storeAlgorithm"), "SHA-256");
        assertEquals(
            hsProperties.get("storeMetadataNamespace"),
            "https://ns.dataone.org/service/types/v2.0#SystemMetadata");
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
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

        new FileHashStore(storeProperties);
    }

    /**
     * Test existing configuration file will raise exception when algorithm is different when
     * instantiating FileHashStore
     */
    @Test
    public void testExistingHashStoreConfiguration_diffAlgorithm() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "MD5");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test existing configuration file will raise exception when depth is different when
     * instantiating FileHashStore
     */
    @Test
    public void testExistingHashStoreConfiguration_diffDepth() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "2");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test existing configuration file will raise exception when width is different when
     * instantiating FileHashStore
     */
    @Test
    public void testExistingHashStoreConfiguration_diffWidth() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "1");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Test existing configuration file will raise exception when metadata formatId is different
     * when instantiating FileHashStore
     */
    @Test
    public void testExistingHashStoreConfiguration_diffMetadataNamespace() {
        assertThrows(IllegalArgumentException.class, () -> {
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", rootDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace", "http://ns.test.org/service/types/v2.0");

            new FileHashStore(storeProperties);
        });
    }

    /**
     * Check that exception is raised when HashStore present but missing configuration file
     * 'hashstore.yaml'
     */
    @Test
    public void testExistingHashStoreConfiguration_missingYaml() {
        assertThrows(IllegalStateException.class, () -> {
            // Create separate store
            Path newStoreDirectory = rootDirectory.resolve("test");
            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", newStoreDirectory.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace",
                "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

            HashStore secondHashStore = new FileHashStore(storeProperties);

            // Confirm config present
            Path newStoreHashStoreYaml = newStoreDirectory.resolve("hashstore.yaml");
            assertTrue(Files.exists(newStoreHashStoreYaml));

            // Store objects
            for (String pid : testData.pidList) {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                    secondHashStore.storeObject(dataStream, pid, null, null, null, -1);
                }
            }

            // Delete configuration
            Files.delete(newStoreHashStoreYaml);

            // Instantiate second HashStore
            new FileHashStore(storeProperties);
        });
    }
}

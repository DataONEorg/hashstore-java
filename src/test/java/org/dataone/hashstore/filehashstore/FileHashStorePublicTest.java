package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for FileHashStore constructor
 */
public class FileHashStorePublicTest {
    private static Path objStringFull;
    private static Path tmpStringFull;
    private static Path rootPathFull;

    /**
     * Initialize FileHashStore
     */
    @BeforeClass
    public static void initializeFileHashStore() {
        Path rootDirectory = tempFolder.getRoot().toPath();
        String rootString = rootDirectory.toString();
        String rootStringFull = rootString + "/metacat";
        objStringFull = Paths.get(rootStringFull + "/objects");
        tmpStringFull = Paths.get(rootStringFull + "/objects/tmp");
        rootPathFull = Paths.get(rootStringFull);

        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootPathFull);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");

        try {
            new FileHashStore(storeProperties);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
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
        Path checkTmpPath = tmpStringFull;
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Test invalid depth value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalDepthArg() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootPathFull);
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
        storeProperties.put("storePath", rootPathFull);
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
        storeProperties.put("storePath", rootPathFull);
        storeProperties.put("storeDepth", 2);
        storeProperties.put("storeWidth", 0);
        storeProperties.put("storeAlgorithm", "SM2");
        new FileHashStore(storeProperties);
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void initDefaultStore_directoryNull() throws Exception {
        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", null);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        new FileHashStore(storeProperties);

        String rootDirectory = System.getProperty("user.dir");
        String objectPath = "FileHashStore";

        Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
        assertTrue(Files.exists(defaultObjDirectoryPath));

        Path defaultTmpDirectoryPath = defaultObjDirectoryPath.resolve("tmp");
        assertTrue(Files.exists(defaultTmpDirectoryPath));

        // Delete the folders
        Files.deleteIfExists(defaultTmpDirectoryPath);
        Files.deleteIfExists(defaultObjDirectoryPath);
    }
}

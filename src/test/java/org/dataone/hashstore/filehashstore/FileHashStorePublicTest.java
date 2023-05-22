package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        try {
            new FileHashStore(3, 2, "SHA-256", rootPathFull);
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
        new FileHashStore(0, 2, "SHA-256", rootPathFull);
    }

    /**
     * Test invalid width value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalWidthArg() throws Exception {
        new FileHashStore(2, 0, "SHA-256", rootPathFull);
    }

    /**
     * Test unsupported algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalAlgorithmArg() throws Exception {
        new FileHashStore(2, 2, "SM2", rootPathFull);
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void initDefaultStore_directoryNull() throws Exception {
        new FileHashStore(3, 2, "SHA-256", null);

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

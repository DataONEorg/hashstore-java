package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for FileHashStore public members
 */
public class FileHashStorePublicTest {
    public FileHashStore fileHashStore;
    public Path objStringFull;
    public Path tmpStringFull;
    public Path rootPathFull;

    /**
     * Initialize FileHashStore for test efficiency purposes (creates directories)
     */
    @Before
    public void initializeFileHashStore() {
        Path rootDirectory = this.tempFolder.getRoot().toPath();
        String rootString = rootDirectory.toString();
        String rootStringFull = rootString + "/metacat";
        this.objStringFull = Paths.get(rootStringFull + "/objects");
        this.tmpStringFull = Paths.get(rootStringFull + "/objects/tmp");
        this.rootPathFull = Paths.get(rootStringFull);
        try {
            this.fileHashStore = new FileHashStore(3, 2, "SHA-256", rootPathFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check object store directory are created after initialization
     */
    @Test
    public void createObjDirectory() {
        Path checkStorePath = this.objStringFull;
        assertTrue(Files.exists(checkStorePath));
    }

    /**
     * Check object store tmp directory are created after initialization
     */
    @Test
    public void createObjTmpDirectory() {
        Path checkTmpPath = this.tmpStringFull;
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Test invalid depth value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalDepthArg() throws Exception {
        new FileHashStore(0, 2, "SHA-256", this.rootPathFull);
    }

    /**
     * Test unsupported algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalAlgorithmArg() throws Exception {
        new FileHashStore(2, 2, "SM2", this.rootPathFull);
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void defaultStore_directoryNull() throws Exception {
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

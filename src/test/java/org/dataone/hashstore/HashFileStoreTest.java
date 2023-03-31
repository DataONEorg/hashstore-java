package org.dataone.hashstore;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.dataone.hashstore.hashfs.HashAddress;
import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * Test class for HashFileStore
 */
public class HashFileStoreTest {
    HashFileStore hfs;
    Path rootDirectory;
    String rootString;
    String rootStringFull;
    String tmpStringFull;

    @Before
    public void initializeHashFileStore() {
        this.rootDirectory = tempFolder.getRoot().toPath();
        this.rootString = rootDirectory.toString();
        this.rootStringFull = rootString + "/metacat/objects";
        this.tmpStringFull = this.rootString + "/metacat/objects/tmp";
        try {
            this.hfs = new HashFileStore(3, 2, "sha256", rootStringFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check object store and tmp directory are created
     */
    @Test
    public void testCreateDirectory() {
        Path checkStorePath = Paths.get(this.rootStringFull);
        assertTrue(Files.exists(checkStorePath));

        Path checkTmpPath = Paths.get(this.tmpStringFull);
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Verify that file was put (moved) to its permanent address
     */
    @Test
    public void testPut() {
        // Get test file to "upload"
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", "jtao.1700.1");
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        try {
            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.put(dataStream, null, null);
            File objAbsPath = new File(address.getAbsPath());
            assertTrue(objAbsPath.exists());
        } catch (NoSuchAlgorithmException e) {
            fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }
}

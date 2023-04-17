package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for HashFileStore
 */
public class HashFileStoreTest {
    public HashFileStore hfs;
    public Path rootDirectory;
    public String rootString;
    public String rootStringFull;
    public String objStringFull;
    public String tmpStringFull;

    public TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize HashFileStore for test efficiency purposes (creates directories)
     */
    @Before
    public void initializeHashFileStore() {
        this.rootDirectory = this.tempFolder.getRoot().toPath();
        this.rootString = this.rootDirectory.toString();
        this.rootStringFull = this.rootString + "/metacat";
        this.objStringFull = this.rootStringFull + "/objects";
        this.tmpStringFull = this.rootStringFull + "/objects/tmp";
        try {
            this.hfs = new HashFileStore(3, 2, "SHA-256", rootStringFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check object store and tmp directory are created after initialization
     */
    @Test
    public void testCreateDirectory() {
        Path checkStorePath = Paths.get(this.objStringFull);
        assertTrue(Files.exists(checkStorePath));

        Path checkTmpPath = Paths.get(this.tmpStringFull);
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Test invalid depth value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalDepthArg() {
        try {
            new HashFileStore(0, 2, "SHA-256", rootStringFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Test invalid algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalAlgorithmArg() {
        try {
            new HashFileStore(2, 2, "SM2", rootStringFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void testDefaultStoreDirectoryNull() {
        try {
            HashFileStore defaultHfs = new HashFileStore(3, 2, "SHA-256", null);

            String rootDirectory = System.getProperty("user.dir");
            String objectPath = "HashFileStore";

            Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
            assertTrue(Files.exists(defaultObjDirectoryPath));

            Path defaultTmpDirectoryPath = defaultObjDirectoryPath.resolve("tmp");
            assertTrue(Files.exists(defaultTmpDirectoryPath));
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Confirm default file directories are created when storeDirectory is ""
     */
    @Test
    public void testDefaultStoreDirectoryEmptyString() {
        try {
            HashFileStore defaultHfs = new HashFileStore(3, 2, "SHA-256", "");

            String rootDirectory = System.getProperty("user.dir");
            String objectPath = "HashFileStore";

            Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
            assertTrue(Files.exists(defaultObjDirectoryPath));

            Path defaultTmpDirectoryPath = defaultObjDirectoryPath.resolve("tmp");
            assertTrue(Files.exists(defaultTmpDirectoryPath));
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address
     */
    @Test
    public void testPutTestHarness() {
        HashUtil hsil = new HashUtil();

        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            try {
                InputStream dataStream = new FileInputStream(testDataFile);
                HashAddress address = hfs.putObject(dataStream, pid, null, null);

                // Check id (sha-256 hex digest of the ab_id, aka s_cid)
                String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
                assertEquals(objAuthorityId, address.getId());

                // Check relative path
                String objRelPath = hsil.shard(3, 2, objAuthorityId);
                assertEquals(objRelPath, address.getRelPath());

                // Check absolute path
                File objAbsPath = new File(address.getAbsPath());
                assertTrue(objAbsPath.exists());

                // Check duplicate status
                assertFalse(address.getIsDuplicate());

            } catch (NoSuchAlgorithmException e) {
                fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
            } catch (IOException e) {
                fail("IOException: " + e.getMessage());
            }
        }
    }

    /**
     * Verify that file was not moved if object exists
     */
    @Test
    public void testPutDuplicateObject() {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        try {
            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null);

            // Check duplicate status
            assertFalse(address.getIsDuplicate());

            // Try duplicate upload
            InputStream dataStreamTwo = new FileInputStream(testDataFile);
            HashAddress addressTwo = hfs.putObject(dataStreamTwo, pid, null, null);
            assertTrue(addressTwo.getIsDuplicate());

            // Confirm there is only 1 file
            File addressAbsPath = new File(address.getAbsPath());
            File addressParent = new File(addressAbsPath.getParent());
            int fileCount = addressParent.list().length;
            assertEquals(fileCount, 1);

        } catch (NoSuchAlgorithmException e) {
            fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutInvalidAlgorithm() {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        try {
            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, "SM2", null);

        } catch (NoSuchAlgorithmException e) {
            fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutIncorrectChecksum() {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        try {
            String checksumInvalid = "1c25df1c8ba1d2e57bb3fd4785878b85";
            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, "MD2", checksumInvalid);

        } catch (NoSuchAlgorithmException e) {
            fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }
}

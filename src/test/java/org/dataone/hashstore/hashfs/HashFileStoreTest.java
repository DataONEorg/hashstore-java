package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

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
    public HashUtil hsil = new HashUtil();

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
     * Check object store directory are created after initialization
     */
    @Test
    public void testCreateObjDirectory() {
        Path checkStorePath = Paths.get(this.objStringFull);
        assertTrue(Files.exists(checkStorePath));
    }

    /**
     * Check object store tmp directory are created after initialization
     */
    @Test
    public void testCreateObjTmpDirectory() {
        Path checkTmpPath = Paths.get(this.tmpStringFull);
        assertTrue(Files.exists(checkTmpPath));
    }

    /**
     * Test invalid depth value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalDepthArg() throws Exception {
        new HashFileStore(0, 2, "SHA-256", rootStringFull);
    }

    /**
     * Test invalid algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorIllegalAlgorithmArg() throws Exception {
        new HashFileStore(2, 2, "SM2", rootStringFull);
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void testDefaultStoreDirectoryNull() throws Exception {
        HashFileStore defaultHfs = new HashFileStore(3, 2, "SHA-256", null);

        String rootDirectory = System.getProperty("user.dir");
        String objectPath = "HashFileStore";

        Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
        assertTrue(Files.exists(defaultObjDirectoryPath));

        Path defaultTmpDirectoryPath = defaultObjDirectoryPath.resolve("tmp");
        assertTrue(Files.exists(defaultTmpDirectoryPath));
    }

    /**
     * Confirm default obj file directory is created when storeDirectory is ""
     */
    @Test
    public void testDefaultObjStoreDirectoryEmptyString() throws Exception {
        HashFileStore defaultHfs = new HashFileStore(3, 2, "SHA-256", "");

        String rootDirectory = System.getProperty("user.dir");
        String objectPath = "HashFileStore";

        Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
        assertTrue(Files.exists(defaultObjDirectoryPath));
    }

    /**
     * Confirm default obj tmp file directory is created when storeDirectory is ""
     */
    @Test
    public void testDefaultObjTmpStoreDirectoryEmptyString() throws Exception {
        HashFileStore defaultHfs = new HashFileStore(3, 2, "SHA-256", "");

        String rootDirectory = System.getProperty("user.dir");
        String objectPath = "HashFileStore";

        Path defaultTmpDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects").resolve("tmp");
        assertTrue(Files.exists(defaultTmpDirectoryPath));
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * authority based id is correct
     */
    @Test
    public void testPutTestHarnessId() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, address.getId());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * relative path is correct
     */
    @Test
    public void testPutTestHarnessRelPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

            // Check relative path
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            String objRelPath = hsil.shard(3, 2, objAuthorityId);
            assertEquals(objRelPath, address.getRelPath());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * absolute path is correct
     */
    @Test
    public void testPutTestHarnessAbsPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

            // Check absolute path
            File objAbsPath = new File(address.getAbsPath());
            assertTrue(objAbsPath.exists());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * isDuplicate is correct
     */
    @Test
    public void testPutTestHarnessIsDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

            // Check duplicate status
            assertFalse(address.getIsDuplicate());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * hex digests are correct
     */
    @Test
    public void testPutTestHarnessHexDigests() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

            Map<String, String> hexDigests = address.getHexDigests();

            // Validate checksum values
            String md5 = this.testData.pidData.get(pid).get("md5");
            String sha1 = this.testData.pidData.get(pid).get("sha1");
            String sha256 = this.testData.pidData.get(pid).get("sha256");
            String sha384 = this.testData.pidData.get(pid).get("sha384");
            String sha512 = this.testData.pidData.get(pid).get("sha512");
            assertEquals(md5, hexDigests.get("MD5"));
            assertEquals(sha1, hexDigests.get("SHA-1"));
            assertEquals(sha256, hexDigests.get("SHA-256"));
            assertEquals(sha384, hexDigests.get("SHA-384"));
            assertEquals(sha512, hexDigests.get("SHA-512"));
        }
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    public void testPutCorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pid, "MD2", checksumCorrect, "MD2");

        String md2 = this.testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutIncorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pid, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when checksum is empty and algorithm supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutEmptyChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        String checksumEmpty = "";
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pid, "MD2", checksumEmpty, "MD2");
    }

    /**
     * Verify that putObject throws exception when storing a duplicate object
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void testPutDuplicateObjectRevised() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        // try {
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pid, null, null, null);

        // Check duplicate status
        assertFalse(address.getIsDuplicate());

        // Try duplicate upload
        InputStream dataStreamTwo = new FileInputStream(testDataFile);
        HashAddress addressTwo = hfs.putObject(dataStreamTwo, pid, null, null, null);
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutInvalidAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pid, "SM2", null, null);
    }

    /**
     * Verify exception thrown when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutEmptyPid() throws Exception {
        // Get test file to "upload"
        String pidEmpty = "";
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pidEmpty, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when pid is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void testPutNullPid() throws Exception {
        // Get test file to "upload"
        String pidNull = null;
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";
        InputStream dataStream = new FileInputStream(testDataFile);
        HashAddress address = hfs.putObject(dataStream, pidNull, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when object is null
     */
    @Test(expected = NullPointerException.class)
    public void testPutNullObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";
        HashAddress address = hfs.putObject(null, pid, "MD2", checksumIncorrect, "MD2");
    }
}

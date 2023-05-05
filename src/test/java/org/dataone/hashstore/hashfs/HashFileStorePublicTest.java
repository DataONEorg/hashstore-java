package org.dataone.hashstore.hashfs;

import java.io.File;
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
 * Test class for HashFileStore public members
 */
public class HashFileStorePublicTest {
    public HashFileStore hashFileStore;
    public Path objStringFull;
    public Path tmpStringFull;
    public Path rootPathFull;
    public TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize HashFileStore for test efficiency purposes (creates directories)
     */
    @Before
    public void initializeHashFileStore() {
        Path rootDirectory = this.tempFolder.getRoot().toPath();
        String rootString = rootDirectory.toString();
        String rootStringFull = rootString + "/metacat";
        this.objStringFull = Paths.get(rootStringFull + "/objects");
        this.tmpStringFull = Paths.get(rootStringFull + "/objects/tmp");
        this.rootPathFull = Paths.get(rootStringFull);
        try {
            this.hashFileStore = new HashFileStore(3, 2, "SHA-256", rootPathFull);
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
        new HashFileStore(0, 2, "SHA-256", this.rootPathFull);
    }

    /**
     * Test invalid algorithm value
     */
    @Test(expected = IllegalArgumentException.class)
    public void constructor_illegalAlgorithmArg() throws Exception {
        new HashFileStore(2, 2, "SM2", this.rootPathFull);
    }

    /**
     * Confirm default file directories are created when storeDirectory is null
     */
    @Test
    public void defaultStore_directoryNull() throws Exception {
        new HashFileStore(3, 2, "SHA-256", null);

        String rootDirectory = System.getProperty("user.dir");
        String objectPath = "HashFileStore";

        Path defaultObjDirectoryPath = Paths.get(rootDirectory).resolve(objectPath).resolve("objects");
        assertTrue(Files.exists(defaultObjDirectoryPath));

        Path defaultTmpDirectoryPath = defaultObjDirectoryPath.resolve("tmp");
        assertTrue(Files.exists(defaultTmpDirectoryPath));
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * authority based id is correct
     */
    @Test
    public void putObject_testHarness_id() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = Paths.get(testdataAbsolutePath);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

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
    public void putObject_testHarness_relPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = Paths.get(testdataAbsolutePath);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

            // Check relative path
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            String objRelPath = hashFileStore.getHierarchicalPathString(3, 2, objAuthorityId);
            assertEquals(objRelPath, address.getRelPath());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * absolute path is correct
     */
    @Test
    public void putObject_testHarness_absPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = Paths.get(testdataAbsolutePath);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

            // Check absolute path
            File objAbsPath = new File(address.getAbsPath());
            assertTrue(objAbsPath.exists());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * isDuplicate is false
     */
    @Test
    public void putObject_testHarness_isDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = Paths.get(testdataAbsolutePath);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

            // Check duplicate status
            assertFalse(address.getIsDuplicate());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * hex digests are correct
     */
    @Test
    public void putObject_testHarness_hexDigests() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = Paths.get(testdataAbsolutePath);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

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
     * Verify that additional checksum is generated/validated
     */
    @Test
    public void putObject_correctChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", checksumCorrect, "MD2");

        String md2 = this.testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_incorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when checksum is empty and algorithm supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", "   ", "MD2");
    }

    /**
     * Verify exception thrown when checksum is null and algorithm supported
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", null, "MD2");
    }

    /**
     * Verify exception thrown when checksumAlgorithm is empty and checksum is
     * supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", "abc", "   ");
    }

    /**
     * Verify exception thrown when checksumAlgorithm is null and checksum supplied
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "MD2", "abc", null);
    }

    /**
     * Verify that putObject throws exception when storing a duplicate object
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void putObject_duplicateObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        HashAddress address = hashFileStore.putObject(dataStream, pid, null, null, null);

        // Check duplicate status
        assertFalse(address.getIsDuplicate());

        // Try duplicate upload
        InputStream dataStreamTwo = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStreamTwo, pid, null, null, null);
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_invalidAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "SM2", null, null);
    }

    /**
     * Verify exception thrown when empty algorithm is supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pid, "   ", null, null);
    }

    /**
     * Verify exception thrown when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyPid() throws Exception {
        // Get test file to "upload"
        String pidEmpty = "";
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, pidEmpty, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when pid is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_nullPid() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = Paths.get(testdataAbsolutePath);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashFileStore.putObject(dataStream, null, "MD2", checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when object is null
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";
        hashFileStore.putObject(null, pid, "MD2", checksumIncorrect, "MD2");
    }
}

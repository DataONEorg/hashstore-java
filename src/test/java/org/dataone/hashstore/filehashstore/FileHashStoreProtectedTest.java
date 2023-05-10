package org.dataone.hashstore.filehashstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
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
 * Test class for FileHashStore protected members
 */
public class FileHashStoreProtectedTest {
    public FileHashStore fileHashStore;
    public Path objStringFull;
    public Path tmpStringFull;
    public Path rootPathFull;
    public TestDataHarness testData = new TestDataHarness();

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

    /*
     * Non-test method using HashUtil class to generate a temp file
     */
    public File generateTemporaryFile() throws Exception {
        Path directory = tempFolder.getRoot().toPath();
        File newFile = this.fileHashStore.generateTmpFile("testfile", directory);
        return newFile;
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check algorithm support for supported algorithm
     */
    @Test
    public void isValidAlgorithm_supported() {
        String md2 = "MD2";
        boolean supported = this.fileHashStore.isValidAlgorithm(md2);
        assertTrue(supported);
    }

    /**
     * Check algorithm support for unsupported algorithm
     */
    @Test
    public void isValidAlgorithm_notSupported() {
        String sm3 = "SM3";
        boolean not_supported = this.fileHashStore.isValidAlgorithm(sm3);
        assertFalse(not_supported);
    }

    /**
     * Check algorithm support for unsupported algorithm with lower cases
     */
    @Test
    public void isValidAlgorithm_notSupportedLowerCase() {
        // Must match string to reduce complexity, no string formatting
        String md2_lowercase = "md2";
        boolean lowercase_not_supported = this.fileHashStore.isValidAlgorithm(md2_lowercase);
        assertFalse(lowercase_not_supported);
    }

    /**
     * Check algorithm support for null algorithm
     */
    @Test(expected = NullPointerException.class)
    public void isValidAlgorithm_algorithmNull() {
        this.fileHashStore.isValidAlgorithm(null);
    }

    /**
     * Confirm that a temporary file has been generated.
     */
    @Test
    public void generateTempFile() throws Exception {
        File newTmpFile = generateTemporaryFile();
        assertTrue(newTmpFile.exists());
    }

    /**
     * Confirm that a digest is sharded appropriately
     */
    @Test
    public void getHierarchicalPathString() {
        String shardedPath = this.fileHashStore.getHierarchicalPathString(3, 2,
                "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        String shardedPathExpected = "94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
    }

    /**
     * Check for correct hex digest value
     */
    @Test
    public void getPidHexDigest() throws Exception {
        for (String pid : this.testData.pidList) {
            String abIdDigest = this.fileHashStore.getPidHexDigest(pid, "SHA-256");
            String abIdTestData = this.testData.pidData.get(pid).get("s_cid");
            assertEquals(abIdDigest, abIdTestData);
        }
    }

    /**
     * Check for NoSuchAlgorithmException
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void getPidHexDigest_badAlgorithm() throws Exception {
        for (String pid : this.testData.pidList) {
            this.fileHashStore.getPidHexDigest(pid, "SM2");
        }
    }

    /**
     * Check that checksums are generated.
     */
    @Test
    public void writeToTempFileAndGenerateChecksums() throws Exception {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream,
                    addAlgo);

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
     * Check that the temporary file that has been written into is not empty
     */
    @Test
    public void writeToTempFileAndGenerateChecksums_tmpFileSize() throws Exception {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo);

            long testDataFileSize = Files.size(testDataFile);
            Path tmpFilePath = newTmpFile.toPath();
            long tmpFileSize = Files.size(tmpFilePath);
            assertEquals(testDataFileSize, tmpFileSize);
        }
    }

    /**
     * Check that additional algorithm is generated and correct
     */
    @Test
    public void writeToTempFileAndGenerateChecksums_additionalAlgo() throws Exception {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream,
                    addAlgo);

            // Validate additional algorithm
            String md2 = this.testData.pidData.get(pid).get("md2");
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Check that exception is thrown when unsupported algorithm supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void writeToTempFileAndGenerateChecksums_invalidAlgo() throws Exception {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            // Extra algo to calculate - MD2
            String addAlgo = "SM2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo);
        }
    }

    /**
     * Confirm that object has moved
     */
    @Test
    public void testMove() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);

        this.fileHashStore.move(newTmpFile, targetFile);
        assertTrue(targetFile.exists());
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown when target already exists
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void testMove_targetExists() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        this.fileHashStore.move(newTmpFile, targetFile);

        File newTmpFileTwo = generateTemporaryFile();
        this.fileHashStore.move(newTmpFileTwo, targetFile);
    }
}
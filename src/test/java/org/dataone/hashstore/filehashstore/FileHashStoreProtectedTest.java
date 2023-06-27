package org.dataone.hashstore.filehashstore;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.dataone.hashstore.HashAddress;
import org.dataone.hashstore.exceptions.PidObjectExistsException;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for FileHashStore protected members
 */
public class FileHashStoreProtectedTest {
    private FileHashStore fileHashStore;
    private HashMap<String, Object> fhsProperties;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize each FileHashStore test with a new root temporary folder
     */
    @Before
    public void initializeFileHashStore() {
        Path rootDirectory = this.tempFolder.getRoot().toPath().resolve("metacat");

        HashMap<String, Object> storeProperties = new HashMap<>();
        storeProperties.put("storePath", rootDirectory);
        storeProperties.put("storeDepth", 3);
        storeProperties.put("storeWidth", 2);
        storeProperties.put("storeAlgorithm", "SHA-256");
        storeProperties.put("storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0");

        try {
            this.fhsProperties = storeProperties;
            this.fileHashStore = new FileHashStore(storeProperties);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
    }

    /*
     * Non-test method using to generate a temp file
     */
    public File generateTemporaryFile() throws Exception {
        Path directory = tempFolder.getRoot().toPath();
        // newFile
        return this.fileHashStore.generateTmpFile("testfile", directory);
    }

    /**
     * Temporary folder for tests to run in
     */
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check algorithm support for supported algorithm
     */
    @Test
    public void isValidAlgorithm_supported() {
        try {
            String md2 = "MD2";
            boolean supported = this.fileHashStore.validateAlgorithm(md2);
            assertTrue(supported);
        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
    }

    /**
     * Check algorithm support for unsupported algorithm
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void isValidAlgorithm_notSupported() throws NoSuchAlgorithmException {
        try {
            String sm3 = "SM3";
            boolean not_supported = this.fileHashStore.validateAlgorithm(sm3);
            assertFalse(not_supported);
        } catch (NoSuchAlgorithmException nsae) {
            throw new NoSuchAlgorithmException("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
    }

    /**
     * Check algorithm support for unsupported algorithm with lower cases
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void isValidAlgorithm_notSupportedLowerCase() throws NoSuchAlgorithmException {
        try {
            // Must match string to reduce complexity, no string formatting
            String md2_lowercase = "md2";
            boolean lowercase_not_supported = this.fileHashStore.validateAlgorithm(md2_lowercase);
            assertFalse(lowercase_not_supported);
        } catch (NoSuchAlgorithmException nsae) {
            throw new NoSuchAlgorithmException("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
    }

    /**
     * Check algorithm support for null algorithm
     */
    @Test(expected = NullPointerException.class)
    public void isValidAlgorithm_algorithmNull() {
        try {
            this.fileHashStore.validateAlgorithm(null);
        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());
        }
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
        for (String pid : testData.pidList) {
            String abIdDigest = this.fileHashStore.getPidHexDigest(pid, "SHA-256");
            String abIdTestData = testData.pidData.get(pid).get("s_cid");
            assertEquals(abIdDigest, abIdTestData);
        }
    }

    /**
     * Check for NoSuchAlgorithmException
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void getPidHexDigest_badAlgorithm() throws Exception {
        for (String pid : testData.pidList) {
            this.fileHashStore.getPidHexDigest(pid, "SM2");
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * authority based id is correct
     */
    @Test
    public void putObject_testHarness_id() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, address.getId());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * relative path is correct
     */
    @Test
    public void putObject_testHarness_relPath() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

            // Check relative path
            String objAuthorityId = testData.pidData.get(pid).get("s_cid");
            String objRelPath = fileHashStore.getHierarchicalPathString(3, 2, objAuthorityId);
            assertEquals(objRelPath, address.getRelPath());
        }
    }

    /**
     * Verify that test data files are put (moved) to its permanent address and
     * absolute path is correct
     */
    @Test
    public void putObject_testHarness_absPath() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

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
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

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
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

            Map<String, String> hexDigests = address.getHexDigests();

            // Validate checksum values
            String md5 = testData.pidData.get(pid).get("md5");
            String sha1 = testData.pidData.get(pid).get("sha1");
            String sha256 = testData.pidData.get(pid).get("sha256");
            String sha384 = testData.pidData.get(pid).get("sha384");
            String sha512 = testData.pidData.get(pid).get("sha512");
            assertEquals(md5, hexDigests.get("MD5"));
            assertEquals(sha1, hexDigests.get("SHA-1"));
            assertEquals(sha256, hexDigests.get("SHA-256"));
            assertEquals(sha384, hexDigests.get("SHA-384"));
            assertEquals(sha512, hexDigests.get("SHA-512"));
        }
    }

    /**
     * Verify that putObject stores object with good checksum value
     */
    @Test
    public void putObject_validateChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        HashAddress address = fileHashStore.putObject(dataStream, pid, null, checksumCorrect, "MD2");

        File objAbsPath = new File(address.getAbsPath());
        assertTrue(objAbsPath.exists());
    }

    /**
     * Verify that additional checksum is generated/validated
     */
    @Test
    public void putObject_additionalAlgo_correctChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "MD2", null, null);

        String md2 = testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_incorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, checksumIncorrect, "MD2");
    }

    /**
     * Verify exception thrown when checksum is empty and algorithm supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "   ", "MD2");
    }

    /**
     * Verify exception thrown when checksum is null and algorithm supported
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, null, "MD2");
    }

    /**
     * Verify exception thrown when checksumAlgorithm is empty and checksum is
     * supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "abc", "   ");
    }

    /**
     * Verify exception thrown when checksumAlgorithm is null and checksum supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_nullChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);
        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "abc", null);
    }

    /**
     * Verify that putObject throws exception when storing a duplicate object
     */
    @Test(expected = PidObjectExistsException.class)
    public void putObject_duplicateObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        HashAddress address = fileHashStore.putObject(dataStream, pid, null, null, null);

        // Check duplicate status
        assertFalse(address.getIsDuplicate());

        // Try duplicate upload
        InputStream dataStreamTwo = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStreamTwo, pid, null, null, null);
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void putObject_invalidAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "SM2", null, null);
    }

    /**
     * Verify exception thrown when empty algorithm is supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "   ", null, null);
    }

    /**
     * Verify exception thrown when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyPid() throws Exception {
        // Get test file to "upload"
        String pidEmpty = "";
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pidEmpty, null, null, null);
    }

    /**
     * Verify exception thrown when pid is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_nullPid() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, null, "MD2", null, null);
    }

    /**
     * Verify exception thrown when object is null
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";

        fileHashStore.putObject(null, pid, "MD2", null, null);
    }

    /**
     * Check that default checksums are generated
     */
    @Test
    public void writeToTmpFileAndGenerateChecksums() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream, null, null);

            // Validate checksum values
            String md5 = testData.pidData.get(pid).get("md5");
            String sha1 = testData.pidData.get(pid).get("sha1");
            String sha256 = testData.pidData.get(pid).get("sha256");
            String sha384 = testData.pidData.get(pid).get("sha384");
            String sha512 = testData.pidData.get(pid).get("sha512");
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
    public void writeToTmpFileAndGenerateChecksums_tmpFileSize() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo, null);

            long testDataFileSize = Files.size(testDataFile);
            long tmpFileSize = Files.size(newTmpFile.toPath());
            assertEquals(testDataFileSize, tmpFileSize);
        }
    }

    /**
     * Check that checksums are generated when additional algorithm supplied.
     */
    @Test
    public void writeToTmpFileAndGenerateChecksums_addAlgo() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream, addAlgo, null);

            // Validate checksum values
            String md2 = testData.pidData.get(pid).get("md2");
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Check that checksums are generated when checksum algorithm supplied
     */
    @Test
    public void writeToTmpFileAndGenerateChecksums_checksumAlgo() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Extra algo to calculate - MD2
            String checksumAlgo = "SHA-512/224";

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream, null, checksumAlgo);

            // Validate checksum values
            String sha512224 = testData.pidData.get(pid).get("sha512-224");
            assertEquals(sha512224, hexDigests.get("SHA-512/224"));
        }
    }

    /**
     * Check that checksums are generated when both additional and checksum
     * algorithm supplied
     */
    @Test
    public void writeToTmpFileAndGenerateChecksums_addAlgoChecksumAlgo() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Extra algo to calculate - MD2
            String addAlgo = "MD2";
            String checksumAlgo = "SHA-512/224";

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile,
                    dataStream, addAlgo, checksumAlgo);

            // Validate checksum values
            String md2 = testData.pidData.get(pid).get("md2");
            String sha512224 = testData.pidData.get(pid).get("sha512-224");
            assertEquals(md2, hexDigests.get("MD2"));
            assertEquals(sha512224, hexDigests.get("SHA-512/224"));
        }
    }

    /**
     * Check that exception is thrown when unsupported algorithm supplied
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void writeToTmpFileAndGenerateChecksums_invalidAlgo() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Extra algo to calculate - MD2
            String addAlgo = "SM2";

            InputStream dataStream = Files.newInputStream(testDataFile);
            this.fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo, null);
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

        this.fileHashStore.move(newTmpFile, targetFile, "object");
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
        this.fileHashStore.move(newTmpFile, targetFile, "object");

        File newTmpFileTwo = generateTemporaryFile();
        this.fileHashStore.move(newTmpFileTwo, targetFile, "object");
    }

    /**
     * Confirm that NullPointerException is thrown when entity is null
     */
    @Test(expected = NullPointerException.class)
    public void testMove_entityNull() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        this.fileHashStore.move(newTmpFile, targetFile, null);
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown entity is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMove_entityEmpty() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        this.fileHashStore.move(newTmpFile, targetFile, "");
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown when entity is empty spaces
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMove_entityEmptySpaces() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        this.fileHashStore.move(newTmpFile, targetFile, "     ");
    }

    /**
     * Test putMetadata stores metadata as expected
     */
    @Test
    public void putMetadata() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String metadataCid = this.fileHashStore.putMetadata(metadataStream, pid, null);

            // Get relative path
            String metadataCidShardString = this.fileHashStore.getHierarchicalPathString(3, 2, metadataCid);
            // Get absolute path
            Path storePath = (Path) this.fhsProperties.get("storePath");
            Path metadataCidAbsPath = storePath.resolve("metadata/" + metadataCidShardString);

            assertTrue(Files.exists(metadataCidAbsPath));
        }
    }

    /**
     * Test putMetadata throws exception when metadata is null
     */
    @Test(expected = NullPointerException.class)
    public void putMetadata_metadataNull() throws Exception {
        for (String pid : testData.pidList) {
            this.fileHashStore.putMetadata(null, pid, null);
        }
    }

    /**
     * Test putMetadata throws exception when pid is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void putMetadata_pidNull() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);

            this.fileHashStore.putMetadata(metadataStream, null, null);
        }
    }

    /**
     * Test putMetadata throws exception when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void putMetadata_pidEmpty() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);

            this.fileHashStore.putMetadata(metadataStream, "", null);
        }
    }

    /**
     * Test putMetadata throws exception when pid is empty with spaces
     */
    @Test(expected = IllegalArgumentException.class)
    public void putMetadata_pidEmptySpaces() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);

            this.fileHashStore.putMetadata(metadataStream, "     ", null);
        }
    }

    /**
     * Check that tmp metadata is written with good data
     */
    @Test
    public void writeToTmpMetadataFile() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");
            String formatId = (String) this.fhsProperties.get("storeMetadataNamespace");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            boolean metadataWritten = this.fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream, formatId);
            assertTrue(metadataWritten);
        }
    }

    /**
     * Check that tmp metadata is actually written by verifying file size
     * 
     * Reminder: We cannot do a size comparison directly because the metadata file
     * stored contains the given namespace/formatId as well
     */
    @Test
    public void writeToTmpMetadataFile_tmpFileSize() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");
            String formatId = (String) this.fhsProperties.get("storeMetadataNamespace");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            boolean metadataWritten = this.fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream, formatId);
            assertTrue(metadataWritten);

            long tmpMetadataFileSize = Files.size(newTmpFile.toPath());
            assertTrue(tmpMetadataFileSize > 0);
        }
    }

    /**
     * Check that tmp metadata written contains correct header
     */
    @Test
    public void writeToTmpMetadataFile_header() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");
            String formatId = (String) this.fhsProperties.get("storeMetadataNamespace");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            this.fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream, formatId);

            // Read the header
            FileInputStream metadataInputStream = new FileInputStream(newTmpFile);
            try (Scanner scanner = new Scanner(metadataInputStream, "UTF-8").useDelimiter("\u0000")) {
                String header = scanner.next();
                assertEquals(header, formatId);

            } catch (IllegalArgumentException iae) {
                iae.printStackTrace();
                throw iae;

            }

        }
    }

    /**
     * Check that tmp metadata written contains correct body. This test uses two
     * approaches when reading the metadata file to cross-verify results.
     */
    @Test
    public void writeToTmpMetadataFile_body() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");
            String formatId = (String) this.fhsProperties.get("storeMetadataNamespace");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            // Write it to the tmpFile
            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            this.fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream, formatId);

            // Confirm header and body
            try (FileInputStream metadataInputStream = new FileInputStream(newTmpFile)) {
                // Read the metadata content manually
                ByteArrayOutputStream headerStream = new ByteArrayOutputStream();
                int currentByte;
                // The null character that splits the header/body is consumed in this while loop
                while ((currentByte = metadataInputStream.read()) != -1 && currentByte != 0) {
                    headerStream.write(currentByte);
                }
                String header = headerStream.toString("UTF-8");
                assertEquals(header, formatId);

                ByteArrayOutputStream bodyStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = metadataInputStream.read(buffer)) != -1) {
                    bodyStream.write(buffer, 0, bytesRead);
                }
                String body = bodyStream.toString("UTF-8");

                // Now confirm the body matches via higher level abstraction class 'Scanner'
                InputStream metadataStreamTwo = Files.newInputStream(testMetaDataFile);
                try (Scanner scanner = new Scanner(metadataStreamTwo, "UTF-8").useDelimiter("\u0000")) {
                    String metadataBody = scanner.next();
                    assertEquals(metadataBody, body);

                } catch (IllegalArgumentException iae) {
                    iae.printStackTrace();
                    throw iae;

                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;

            }
        }
    }
}
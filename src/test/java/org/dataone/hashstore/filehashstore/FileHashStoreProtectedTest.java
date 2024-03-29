package org.dataone.hashstore.filehashstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.dataone.hashstore.ObjectInfo;
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
    private Properties fhsProperties;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize each FileHashStore test with a new root temporary folder
     */
    @Before
    public void initializeFileHashStore() {
        Path rootDirectory = tempFolder.getRoot().toPath().resolve("metacat");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0"
        );

        try {
            fhsProperties = storeProperties;
            fileHashStore = new FileHashStore(storeProperties);

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
        return fileHashStore.generateTmpFile("testfile", directory);
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
            boolean supported = fileHashStore.validateAlgorithm(md2);
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
            boolean not_supported = fileHashStore.validateAlgorithm(sm3);
            assertFalse(not_supported);

        } catch (NoSuchAlgorithmException nsae) {
            throw new NoSuchAlgorithmException(
                "NoSuchAlgorithmException encountered: " + nsae.getMessage()
            );

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
            boolean lowercase_not_supported = fileHashStore.validateAlgorithm(md2_lowercase);
            assertFalse(lowercase_not_supported);

        } catch (NoSuchAlgorithmException nsae) {
            throw new NoSuchAlgorithmException(
                "NoSuchAlgorithmException encountered: " + nsae.getMessage()
            );

        }
    }

    /**
     * Check algorithm support for null algorithm value
     */
    @Test(expected = NullPointerException.class)
    public void isValidAlgorithm_algorithmNull() {
        try {
            fileHashStore.validateAlgorithm(null);

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
     * Confirm that a given digest is sharded appropriately
     */
    @Test
    public void getHierarchicalPathString() {
        String shardedPath = fileHashStore.getHierarchicalPathString(
            3, 2, "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a"
        );
        String shardedPathExpected =
            "94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
    }

    /**
     * Check getPidHexDigest calculates correct hex digest value
     */
    @Test
    public void getPidHexDigest() throws Exception {
        for (String pid : testData.pidList) {
            String abIdDigest = fileHashStore.getPidHexDigest(pid, "SHA-256");
            String abIdTestData = testData.pidData.get(pid).get("object_cid");
            assertEquals(abIdDigest, abIdTestData);
        }
    }

    /**
     * Check that getPidHexDigest throws NoSuchAlgorithmException
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void getPidHexDigest_badAlgorithm() throws Exception {
        for (String pid : testData.pidList) {
            fileHashStore.getPidHexDigest(pid, "SM2");
        }
    }

    /**
     * Verify that putObject returns correct id
     */
    @Test
    public void putObject_testHarness_id() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo address = fileHashStore.putObject(dataStream, pid, null, null, null, 0);

            // Check id (sha-256 hex digest of the ab_id, aka object_cid)
            String objAuthorityId = testData.pidData.get(pid).get("object_cid");
            assertEquals(objAuthorityId, address.getId());
        }
    }

    /**
     * Check that store object returns the correct ObjectInfo size
     */
    @Test
    public void putObject_objSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo objInfo = fileHashStore.putObject(dataStream, pid, null, null, null, 0);

            // Check id (sha-256 hex digest of the ab_id (pid))
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Verify that putObject returns correct hex digests
     */
    @Test
    public void putObject_testHarness_hexDigests() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo address = fileHashStore.putObject(dataStream, pid, null, null, null, 0);

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
        ObjectInfo address = fileHashStore.putObject(
            dataStream, pid, null, checksumCorrect, "MD2", 0
        );

        String objCid = address.getId();
        // Get relative path
        String objCidShardString = fileHashStore.getHierarchicalPathString(3, 2, objCid);
        // Get absolute path
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        Path objCidAbsPath = storePath.resolve("objects/" + objCidShardString);

        assertTrue(Files.exists(objCidAbsPath));
    }

    /**
     * Verify putObject generates additional checksum
     */
    @Test
    public void putObject_additionalAlgo_correctChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "MD2", null, null, 0);

        String md2 = testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify putObject throws exception when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_incorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, checksumIncorrect, "MD2", 0);
    }

    /**
     * Verify putObject throws exception when checksum is empty and algorithm supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "   ", "MD2", 0);
    }

    /**
     * Verify putObject throws exception when checksum is null and algorithm supported
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, null, "MD2", 0);
    }

    /**
     * Verify putObject throws exception when checksumAlgorithm is empty and checksum is supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "abc", "   ", 0);
    }

    /**
     * Verify putObject throws exception when checksumAlgorithm is null and checksum supplied
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullChecksumAlgorithmValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);
        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, "abc", null, 0);
    }


    /**
     * Check that store object throws exception when incorrect file size provided
     */
    @Test
    public void putObject_objSizeCorrect() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo objInfo = fileHashStore.putObject(
                dataStream, pid, null, null, null, objectSize
            );

            // Check id (sha-256 hex digest of the ab_id (pid))
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object throws exception when incorrect file size provided
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_objSizeIncorrect() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectInfo objInfo = fileHashStore.putObject(dataStream, pid, null, null, null, 1000);

            // Check id (sha-256 hex digest of the ab_id (pid))
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Verify putObject throws exception when storing a duplicate object
     */
    @Test(expected = PidObjectExistsException.class)
    public void putObject_duplicateObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, null, null, 0);

        // Try duplicate upload
        InputStream dataStreamTwo = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStreamTwo, pid, null, null, null, 0);
    }

    /**
     * Verify putObject throws exception when unsupported additional algorithm provided
     */
    @Test(expected = NoSuchAlgorithmException.class)
    public void putObject_invalidAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "SM2", null, null, 0);
    }

    /**
     * Verify putObject throws exception when empty algorithm is supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, "   ", null, null, 0);
    }

    /**
     * Verify putObject throws exception when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void putObject_emptyPid() throws Exception {
        // Get test file to "upload"
        String pidEmpty = "";
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pidEmpty, null, null, null, 0);
    }

    /**
     * Verify putObject throws exception when pid is null
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullPid() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, null, "MD2", null, null, 0);
    }

    /**
     * Verify putObject throws exception object is null
     */
    @Test(expected = NullPointerException.class)
    public void putObject_nullObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";

        fileHashStore.putObject(null, pid, "MD2", null, null, 0);
    }

    /**
     * Check default checksums are generated
     */
    @Test
    public void writeToTmpFileAndGenerateChecksums() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests = fileHashStore.writeToTmpFileAndGenerateChecksums(
                newTmpFile, dataStream, null, null
            );

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
            fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo, null);

            long testDataFileSize = Files.size(testDataFile);
            long tmpFileSize = Files.size(newTmpFile.toPath());
            assertEquals(testDataFileSize, tmpFileSize);
        }
    }

    /**
     * Check that additional algorithm is generated when supplied
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
            Map<String, String> hexDigests = fileHashStore.writeToTmpFileAndGenerateChecksums(
                newTmpFile, dataStream, addAlgo, null
            );

            // Validate checksum values
            String md2 = testData.pidData.get(pid).get("md2");
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Check that checksums are generated when checksumAlgorithm is supplied
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
            Map<String, String> hexDigests = fileHashStore.writeToTmpFileAndGenerateChecksums(
                newTmpFile, dataStream, null, checksumAlgo
            );

            // Validate checksum values
            String sha512224 = testData.pidData.get(pid).get("sha512-224");
            assertEquals(sha512224, hexDigests.get("SHA-512/224"));
        }
    }

    /**
     * Check that checksums are generated when both additional and checksum algorithm supplied
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
            Map<String, String> hexDigests = fileHashStore.writeToTmpFileAndGenerateChecksums(
                newTmpFile, dataStream, addAlgo, checksumAlgo
            );

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
            fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo, null);
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

        fileHashStore.move(newTmpFile, targetFile, "object");
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
        fileHashStore.move(newTmpFile, targetFile, "object");

        File newTmpFileTwo = generateTemporaryFile();
        fileHashStore.move(newTmpFileTwo, targetFile, "object");
    }

    /**
     * Confirm that NullPointerException is thrown when entity is null
     */
    @Test(expected = NullPointerException.class)
    public void testMove_entityNull() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        fileHashStore.move(newTmpFile, targetFile, null);
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown entity is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMove_entityEmpty() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        fileHashStore.move(newTmpFile, targetFile, "");
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown when entity is empty spaces
     */
    @Test(expected = IllegalArgumentException.class)
    public void testMove_entityEmptySpaces() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        fileHashStore.move(newTmpFile, targetFile, "     ");
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
            String metadataCid = fileHashStore.putMetadata(metadataStream, pid, null);

            // Get relative path
            String metadataCidShardString = fileHashStore.getHierarchicalPathString(
                3, 2, metadataCid
            );
            // Get absolute path
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
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
            fileHashStore.putMetadata(null, pid, null);
        }
    }

    /**
     * Test putMetadata throws exception when pid is null
     */
    @Test(expected = NullPointerException.class)
    public void putMetadata_pidNull() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);

            fileHashStore.putMetadata(metadataStream, null, null);
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

            fileHashStore.putMetadata(metadataStream, "", null);
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

            fileHashStore.putMetadata(metadataStream, "     ", null);
        }
    }

    /**
     * Confirm tmp metadata is written
     */
    @Test
    public void writeToTmpMetadataFile() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            boolean metadataWritten = fileHashStore.writeToTmpMetadataFile(
                newTmpFile, metadataStream
            );
            assertTrue(metadataWritten);
        }
    }

    /**
     * Check that tmp metadata is actually written by verifying file size
     * 
     */
    @Test
    public void writeToTmpMetadataFile_tmpFileSize() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            boolean metadataWritten = fileHashStore.writeToTmpMetadataFile(
                newTmpFile, metadataStream
            );
            assertTrue(metadataWritten);

            long tmpMetadataFileSize = Files.size(newTmpFile.toPath());
            long testMetadataFileSize = Files.size(testMetaDataFile);
            assertTrue(tmpMetadataFileSize > 0);
            assertEquals(tmpMetadataFileSize, testMetadataFileSize);
        }
    }

    /**
     * Check tmp metadata content
     */
    @Test
    public void writeToTmpMetadataFile_metadataContent() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            // Write it to the tmpFile
            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);

            // Create InputStream to tmp File
            InputStream metadataStoredStream;
            try {
                metadataStoredStream = Files.newInputStream(newTmpFile.toPath());

            } catch (Exception e) {
                e.printStackTrace();
                throw e;

            }

            // Calculate checksum of metadata content
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            try {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = metadataStoredStream.read(buffer)) != -1) {
                    sha256.update(buffer, 0, bytesRead);
                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;

            }

            String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
            String sha256MetadataDigestFromTestData = testData.pidData.get(pid).get(
                "metadata_sha256"
            );
            assertEquals(sha256Digest, sha256MetadataDigestFromTestData);

            // Close stream
            metadataStoredStream.close();
        }
    }
}

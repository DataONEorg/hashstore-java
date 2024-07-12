package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.OrphanPidRefsFileException;
import org.dataone.hashstore.exceptions.OrphanRefsFilesException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileNotFoundException;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for FileHashStore protected members
 */
public class FileHashStoreProtectedTest {
    private FileHashStore fileHashStore;
    private Properties fhsProperties;
    private Path rootDirectory;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize each FileHashStore test with a new root temporary folder
     */
    @BeforeEach
    public void initializeFileHashStore() {
        rootDirectory = tempFolder.resolve("hashstore");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

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
        Path directory = tempFolder.resolve("hashstore");
        // newFile
        return FileHashStoreUtility.generateTmpFile("testfile", directory);
    }

    /**
     * Temporary folder for tests to run in
     */
    @TempDir
    public Path tempFolder;

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
    @Test
    public void isValidAlgorithm_notSupported() {
        assertThrows(NoSuchAlgorithmException.class, () -> {
            try {
                String sm3 = "SM3";
                boolean not_supported = fileHashStore.validateAlgorithm(sm3);
                assertFalse(not_supported);

            } catch (NoSuchAlgorithmException nsae) {
                throw new NoSuchAlgorithmException(
                    "NoSuchAlgorithmException encountered: " + nsae.getMessage());

            }
        });
    }

    /**
     * Check algorithm support for unsupported algorithm with lower cases
     */
    @Test
    public void isValidAlgorithm_notSupportedLowerCase() {
        assertThrows(NoSuchAlgorithmException.class, () -> {
            try {
                // Must match string to reduce complexity, no string formatting
                String md2_lowercase = "md2";
                boolean lowercase_not_supported = fileHashStore.validateAlgorithm(md2_lowercase);
                assertFalse(lowercase_not_supported);

            } catch (NoSuchAlgorithmException nsae) {
                throw new NoSuchAlgorithmException(
                    "NoSuchAlgorithmException encountered: " + nsae.getMessage());

            }
        });
    }

    /**
     * Check algorithm support for null algorithm value throws exception
     */
    @Test
    public void isValidAlgorithm_algorithmNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            try {
                fileHashStore.validateAlgorithm(null);

            } catch (NoSuchAlgorithmException nsae) {
                fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

            }
        });
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
        String shardedPath = FileHashStoreUtility.getHierarchicalPathString(3, 2,
                                                                            "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        String shardedPathExpected =
            "94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
    }

    /**
     * Check that findObject returns cid as expected.
     */
    @Test
    public void findObject_cid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            Map<String, String> objInfoMap = fileHashStore.findObject(pid);
            assertEquals(objInfoMap.get("cid"), objInfo.getCid());
        }
    }

    /**
     * Check that findObject returns the path to the object as expected.
     */
    @Test
    public void findObject_cidPath() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            Map<String, String> objInfoMap = fileHashStore.findObject(pid);
            String objectPath = objInfoMap.get("cid_object_path");

            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            Path realPath = rootDirectory.resolve("objects").resolve(objRelativePath);

            assertEquals(objectPath, realPath.toString());
        }
    }

    /**
     * Check that findObject returns the absolute path to the pid and cid refs file
     */
    @Test
    public void findObject_refsPaths() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            Map<String, String> objInfoMap = fileHashStore.findObject(pid);
            String cidRefsPath = objInfoMap.get("cid_refs_path");
            String pidRefsPath = objInfoMap.get("pid_refs_path");

            Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(objInfo.getCid(), "cid");
            Path pidRefsFilePath = fileHashStore.getHashStoreRefsPath(pid, "pid");

            assertEquals(cidRefsPath, cidRefsFilePath.toString());
            assertEquals(pidRefsPath, pidRefsFilePath.toString());
        }
    }

    /**
     * Check that findObject returns the absolute path to sysmeta document if it exists
     */
    @Test
    public void findObject_sysmetaPath_exists() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Store Object
            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            // Store Metadata
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String metadataPath = fileHashStore.storeMetadata(metadataStream, pid);
            metadataStream.close();
            System.out.println(metadataPath);


            Map<String, String> objInfoMap = fileHashStore.findObject(pid);
            String objInfoSysmetaPath = objInfoMap.get("sysmeta_path");

            String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
            Path sysmetaPath = fileHashStore.getHashStoreMetadataPath(pid, storeMetadataNamespace);
            System.out.println(sysmetaPath);

            assertEquals(objInfoSysmetaPath, sysmetaPath.toString());
        }
    }

    /**
     * Check that findObject returns "Does not exist." when there is no sysmeta for the pid.
     */
    @Test
    public void findObject_sysmetaPath_doesNotExist() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();


            Map<String, String> objInfoMap = fileHashStore.findObject(pid);
            String objInfoSysmetaPath = objInfoMap.get("sysmeta_path");

            assertEquals(objInfoSysmetaPath, "Does not exist");
        }
    }

    /**
     * Confirm findObject throws exception when cid object does not exist but reference
     * files exist.
     */
    @Test
    public void findObject_refsFileExistButObjectDoesNot() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        assertThrows(OrphanRefsFilesException.class, () -> fileHashStore.findObject(pid));
    }

    /**
     * Confirm that findObject throws OrphanPidRefsFileException exception when
     * pid refs file found but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        Files.delete(cidRefsPath);

        assertThrows(OrphanPidRefsFileException.class, () -> fileHashStore.findObject(pid));
    }


    /**
     * Confirm that findObject throws PidNotFoundInCidRefsFileException exception when
     * pid refs file found but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileMissingPid() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        fileHashStore.updateRefsFile(pid, cidRefsPath, "remove");

        assertThrows(PidNotFoundInCidRefsFileException.class, () -> fileHashStore.findObject(pid));
    }

    /**
     * Check that exception is thrown when pid refs file doesn't exist
     */
    @Test
    public void findObject_pidNotFound() {
        String pid = "dou.test.1";
        assertThrows(PidRefsFileNotFoundException.class, () -> fileHashStore.findObject(pid));
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
            ObjectMetadata address = fileHashStore.putObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Check id (sha-256 hex digest of the ab_id, aka object_cid)
            String objContentId = testData.pidData.get(pid).get("sha256");
            assertEquals(objContentId, address.getCid());
        }
    }

    /**
     * Check that store object returns the correct ObjectMetadata size
     */
    @Test
    public void putObject_objSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.putObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

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
            ObjectMetadata address = fileHashStore.putObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

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
        ObjectMetadata address = fileHashStore.putObject(dataStream, pid, null, checksumCorrect, "MD2", -1);
        dataStream.close();

        String objCid = address.getCid();
        // Get relative path
        String objCidShardString = FileHashStoreUtility.getHierarchicalPathString(3, 2, objCid);
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
        fileHashStore.putObject(dataStream, pid, "MD2", null, null, -1);
        dataStream.close();

        String md2 = testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify putObject throws exception when checksum provided does not match
     */
    @Test
    public void putObject_incorrectChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, null, checksumIncorrect, "MD2", -1);
            dataStream.close();
        });
    }

    /**
     * Verify putObject throws exception when checksum is empty and algorithm supported
     */
    @Test
    public void putObject_emptyChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, null, "   ", "MD2", -1);
            dataStream.close();
        });
    }

    /**
     * Verify putObject throws exception when checksum is null and algorithm supported
     */
    @Test
    public void putObject_nullChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, null, null, "MD2", -1);
            dataStream.close();
        });
    }

    /**
     * Verify putObject throws exception when checksumAlgorithm is empty and checksum is supplied
     */
    @Test
    public void putObject_emptyChecksumAlgorithmValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, null, "abc", "   ", -1);
            dataStream.close();
        });
    }

    /**
     * Verify putObject throws exception when checksumAlgorithm is null and checksum supplied
     */
    @Test
    public void putObject_nullChecksumAlgorithmValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);
            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, null, "abc", null, -1);
            dataStream.close();
        });
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
            ObjectMetadata objInfo = fileHashStore.putObject(dataStream, pid, null, null, null, objectSize);
            dataStream.close();

            // Check id (sha-256 hex digest of the ab_id (pid))
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object throws exception when incorrect file size provided
     */
    @Test
    public void putObject_objSizeIncorrect() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.putObject(dataStream, pid, null, null, null, 1000);
                dataStream.close();

                // Check id (sha-256 hex digest of the ab_id (pid))
                long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
                assertEquals(objectSize, objInfo.getSize());
            });
        }
    }

    /**
     * Verify putObject deletes temporary file written if called to store an object that already
     * exists (duplicate)
     */
    @Test
    public void putObject_duplicateObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, null, null, -1);
        dataStream.close();

        // Try duplicate upload
        String pidTwo = pid + ".test";
        InputStream dataStreamTwo = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStreamTwo, pidTwo, null, null, null, -1);
        dataStreamTwo.close();

        // Confirm there are no files in 'objects/tmp' directory
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] files = storePath.resolve("objects/tmp").toFile().listFiles();
        assertEquals(0, Objects.requireNonNull(files).length);
    }

    /**
     * Verify putObject throws exception when unsupported additional algorithm provided
     */
    @Test
    public void putObject_invalidAlgorithm() {
        assertThrows(NoSuchAlgorithmException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, "SM2", null, null, -1);
            dataStream.close();
        });
    }

    /**
     * Verify putObject throws exception when empty algorithm is supplied
     */
    @Test
    public void putObject_emptyAlgorithm() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.putObject(dataStream, pid, "   ", null, null, -1);
            dataStream.close();
        });
    }

    /**
     * Confirm verifyChecksumParameters returns true with good values
     */
    @Test
    public void verifyChecksumParameters() throws Exception {
        boolean shouldValidate = fileHashStore.verifyChecksumParameters("abc123","SHA-256");
        assertTrue(shouldValidate);
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum value is empty
     */
    @Test
    public void verifyChecksumParameters_emptyChecksum() {
        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.verifyChecksumParameters("     ","SHA-256");
        });
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum algorithm is empty
     */
    @Test
    public void verifyChecksumParameters_emptyAlgorithm() {
        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.verifyChecksumParameters("abc123","     ");
        });
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum algorithm is not supported
     */
    @Test
    public void verifyChecksumParameters_unsupportedAlgorithm() {
        assertThrows(NoSuchAlgorithmException.class, () -> {
            fileHashStore.verifyChecksumParameters("abc123","SHA-DOU");
        });
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
            Map<String, String> hexDigests =
                fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, null, null);
            dataStream.close();

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
            dataStream.close();

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
            Map<String, String> hexDigests =
                fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo,
                                                                 null);
            dataStream.close();

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
            Map<String, String> hexDigests =
                fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, null,
                                                                 checksumAlgo);
            dataStream.close();

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
            Map<String, String> hexDigests =
                fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, addAlgo,
                                                                 checksumAlgo);
            dataStream.close();

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
    @Test
    public void writeToTmpFileAndGenerateChecksums_invalidAlgo() {
        for (String pid : testData.pidList) {
            assertThrows(NoSuchAlgorithmException.class, () -> {
                File newTmpFile = generateTemporaryFile();
                String pidFormatted = pid.replace("/", "_");

                // Get test file
                Path testDataFile = testData.getTestFile(pidFormatted);

                // Extra algo to calculate - MD2
                String addAlgo = "SM2";

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.writeToTmpFileAndGenerateChecksums(
                    newTmpFile, dataStream, addAlgo, null);
                dataStream.close();
            });
        }
    }

    /**
     * Confirm that object has moved
     */
    @Test
    public void testMove() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);

        fileHashStore.move(newTmpFile, targetFile, "object");
        assertTrue(targetFile.exists());
    }

    /**
     * Confirm that exceptions are not thrown when move is called on an object that already exists
     */
    @Test
    public void move_targetExists() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);
        fileHashStore.move(newTmpFile, targetFile, "object");

        File newTmpFileTwo = generateTemporaryFile();
        fileHashStore.move(newTmpFileTwo, targetFile, "object");
    }

    /**
     * Confirm that NullPointerException is thrown when entity is null
     */
    @Test
    public void move_entityNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            File newTmpFile = generateTemporaryFile();
            String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
            File targetFile = new File(targetString);
            fileHashStore.move(newTmpFile, targetFile, null);
        });
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown entity is empty
     */
    @Test
    public void move_entityEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            File newTmpFile = generateTemporaryFile();
            String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
            File targetFile = new File(targetString);
            fileHashStore.move(newTmpFile, targetFile, "");
        });
    }

    /**
     * Confirm that FileAlreadyExistsException is thrown when entity is empty spaces
     */
    @Test
    public void move_entityEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            File newTmpFile = generateTemporaryFile();
            String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
            File targetFile = new File(targetString);
            fileHashStore.move(newTmpFile, targetFile, "     ");
        });
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
            String metadataPath = fileHashStore.putMetadata(metadataStream, pid, null);
            metadataStream.close();

            // Calculate absolute path
            String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
            Path metadataPidExpectedPath =
                fileHashStore.getHashStoreMetadataPath(pid, storeMetadataNamespace);
            assertEquals(metadataPath, metadataPidExpectedPath.toString());
        }
    }

    /**
     * Test putMetadata throws exception when metadata is null
     */
    @Test
    public void putMetadata_metadataNull() {
        for (String pid : testData.pidList) {
            assertThrows(
                IllegalArgumentException.class, () -> fileHashStore.putMetadata(null, pid, null));
        }
    }

    /**
     * Test putMetadata throws exception when pid is null
     */
    @Test
    public void putMetadata_pidNull() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.putMetadata(metadataStream, null, null);
                metadataStream.close();
            });
        }
    }

    /**
     * Test putMetadata throws exception when pid is empty
     */
    @Test
    public void putMetadata_pidEmpty() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.putMetadata(metadataStream, "", null);
                metadataStream.close();
            });
        }
    }

    /**
     * Test putMetadata throws exception when pid is empty with spaces
     */
    @Test
    public void putMetadata_pidEmptySpaces() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.putMetadata(metadataStream, "     ", null);
                metadataStream.close();
            });
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
            boolean metadataWritten = fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);
            metadataStream.close();
            assertTrue(metadataWritten);
        }
    }

    /**
     * Check that tmp metadata is actually written by verifying file size
     */
    @Test
    public void writeToTmpMetadataFile_tmpFileSize() throws Exception {
        for (String pid : testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            boolean metadataWritten = fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);
            metadataStream.close();
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
            metadataStream.close();

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
                metadataStoredStream.close();

            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;

            }

            String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
            String sha256MetadataDigestFromTestData =
                testData.pidData.get(pid).get("metadata_cid_sha256");
            assertEquals(sha256Digest, sha256MetadataDigestFromTestData);

            // Close stream
            metadataStoredStream.close();
        }
    }

    /**
     * Confirm that isStringInRefsFile returns true when pid is found
     */
    @Test
    public void isStringInRefsFile_pidFound() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            String pidTwo = pid + ".test";
            InputStream dataStreamDup = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStreamDup, pidTwo, null, null, null, -1);
            dataStreamDup.close();

            String cid = objInfo.getCid();
            Path absCidRefsPath = fileHashStore.getHashStoreRefsPath(cid, "cid");
            assertTrue(fileHashStore.isStringInRefsFile(pidTwo, absCidRefsPath));
        }
    }

    /**
     * Confirm that isStringInRefsFile returns false when pid is found
     */
    @Test
    public void isStringInRefsFile_pidNotFound() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            String cid = objInfo.getCid();
            Path absCidRefsPath = fileHashStore.getHashStoreRefsPath(cid, "cid");
            assertFalse(fileHashStore.isStringInRefsFile("pid.not.found", absCidRefsPath));
        }
    }

    /**
     * Confirm deleteObjectByCid method deletes object when there are no references.
     */
    @Test
    public void deleteObjectByCid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            // Store object only
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();
            String cid = objInfo.getCid();

            // Try deleting the object
            fileHashStore.deleteObjectByCid(cid);

            // Get permanent address of the actual cid
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String objShardString = FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);

            Path objRealPath = storePath.resolve("objects").resolve(objShardString);
            assertFalse(Files.exists(objRealPath));
        }
    }

    /**
     * Confirm deleteObjectByCid method does not delete an object if a cid refs file exists (pids
     * still referencing the cid).
     */
    @Test
    public void tryDeleteObjectByCid_cidRefsFileContainsPids() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();
            String cid = objInfo.getCid();

            // Try deleting the object
            fileHashStore.deleteObjectByCid(cid);

            // Get permanent address of the actual cid
            Path objRealPath = fileHashStore.getHashStoreDataObjectPath(pid);
            assertTrue(Files.exists(objRealPath));
        }
    }

    /**
     * Confirm getHashStoreDataObjectPath returns correct object path
     */
    @Test
    public void getHashStoreDataObjectPath() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();
            String cid = objInfo.getCid();

            // Manually form the permanent address of the actual cid
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String objShardString = FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);
            Path calculatedObjRealPath = storePath.resolve("objects").resolve(objShardString);

            Path expectedObjCidAbsPath = fileHashStore.getHashStoreDataObjectPath(pid);

            assertEquals(expectedObjCidAbsPath, calculatedObjRealPath);
        }
    }

    /**
     * Confirm getHashStoreMetadataPath returns correct metadata path
     */
    @Test
    public void getHashStoreMetadataPath() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid);
            metadataStream.close();

            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            String storeFormatId = fhsProperties.getProperty("storeMetadataNamespace");
            String storeAlgo = fhsProperties.getProperty("storeAlgorithm");
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));

            // Document ID
            String hashId = FileHashStoreUtility.getPidHexDigest(pid + storeFormatId, storeAlgo);

            // Metadata directory of the given pid
            String metadataPidDirId = FileHashStoreUtility.getPidHexDigest(pid, storeAlgo);
            String metadataPidDirIdSharded =
                FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth,
                                                               metadataPidDirId);

            // Complete path
            Path calculatedMetadataRealPath =
                storePath.resolve("metadata").resolve(metadataPidDirIdSharded).resolve(hashId);

            Path expectedMetadataPidPath =
                fileHashStore.getHashStoreMetadataPath(pid, storeFormatId);

            assertEquals(expectedMetadataPidPath, calculatedMetadataRealPath);
        }
    }

    /**
     * Check that getHashStoreMetadataInputStream returns an InputStream
     */
    @Test
    public void getHashStoreMetadataInputStream() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");

            InputStream metadataCidInputStream = fileHashStore.getHashStoreMetadataInputStream(pid, storeFormatId);
            assertNotNull(metadataCidInputStream);
        }
    }

    /**
     * Check that getHashStoreMetadataInputStream throws FileNotFoundException when there is no
     * metadata to retrieve
     */
    @Test
    public void getHashStoreMetadataInputStream_fileNotFound() {
        for (String pid : testData.pidList) {
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");

            assertThrows(
                FileNotFoundException.class,
                () -> fileHashStore.getHashStoreMetadataInputStream(pid, storeFormatId));
        }
    }

    /**
     * Confirm getHashStoreRefsPath returns correct pid refs path
     */
    @Test
    public void getHashStoreRefsPath_pid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Manually form the permanent address of the actual cid
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String storeAlgo = fhsProperties.getProperty("storeAlgorithm");

            // Pid refs file
            String metadataPidHash = FileHashStoreUtility.getPidHexDigest(pid, storeAlgo);
            String metadataPidHashSharded =
                FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth,
                                                               metadataPidHash);
            Path calculatedPidRefsRealPath =
                storePath.resolve("refs/pids").resolve(metadataPidHashSharded);

            Path expectedPidRefsPath = fileHashStore.getHashStoreRefsPath(pid, "pid");

            assertEquals(expectedPidRefsPath, calculatedPidRefsRealPath);
        }
    }

    /**
     * Confirm getHashStoreRefsPath returns correct cid refs path
     */
    @Test
    public void getHashStoreRefsPath_cid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();
            String cid = objInfo.getCid();

            // Manually form the permanent address of the actual cid
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));

            // Cid refs file
            String objShardString = FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);
            Path calculatedCidRefsRealPath = storePath.resolve("refs/cids").resolve(objShardString);

            Path expectedCidRefsPath = fileHashStore.getHashStoreRefsPath(cid, "cid");

            assertEquals(expectedCidRefsPath, calculatedCidRefsRealPath);
        }
    }

    /**
     * Confirm getHashStoreRefsPath throws exception when requesting the path to a refs file with a
     * formatId arg that is not "cid" or "pid"
     */
    @Test
    public void getHashStoreRefsPath_incorrectRefsType() {
        assertThrows(IllegalArgumentException.class, () -> {
            String cid = "testcid";
            fileHashStore.getHashStoreRefsPath(cid, "not_cid_or_pid");
        });
    }

    /**
     * Confirm getHashStoreDataObjectPath throws exception when requesting path for an object
     * that does not exist
     */
    @Test
    public void getHashStoreDataObjectPath_fileNotFound() {
        assertThrows(FileNotFoundException.class, () -> {
            String pid = "dou.test.1";
            fileHashStore.getHashStoreDataObjectPath(pid);
        });
    }

    /**
     * Confirm getExpectedPath throws exception when requesting path for an object that does not
     * exist
     */
    @Test
    public void fileHashStoreUtility_checkForEmptyString() {
        assertThrows(
            IllegalArgumentException.class,
            () -> FileHashStoreUtility.checkForEmptyString("dou.test.1\n", "pid", "storeObject"));
    }
}

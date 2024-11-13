package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.CidNotFoundInPidRefsFileException;
import org.dataone.hashstore.exceptions.HashStoreRefsAlreadyExistException;
import org.dataone.hashstore.exceptions.IdentifierNotLockedException;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.OrphanPidRefsFileException;
import org.dataone.hashstore.exceptions.OrphanRefsFilesException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
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
     * Check that findObject returns cid as expected.
     */
    @Test
    public void findObject_cid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objMeta =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                FileHashStore.ObjectInfo findObjInfo = fileHashStore.findObject(pid);
                assertEquals(findObjInfo.cid(), objMeta.cid());
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
                FileHashStore.ObjectInfo findObjInfo = fileHashStore.findObject(pid);
                String objectPath = findObjInfo.cidObjectPath();

                String objRelativePath =
                    FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth,
                                                                   objInfo.cid());
                Path realPath = rootDirectory.resolve("objects").resolve(objRelativePath);

                assertEquals(objectPath, realPath.toString());
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                FileHashStore.ObjectInfo findObjInfo = fileHashStore.findObject(pid);
                String cidRefsPath = findObjInfo.cidRefsPath();
                String pidRefsPath = findObjInfo.pidRefsPath();

                Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(objInfo.cid(),
                                                                          FileHashStore.HashStoreIdTypes.cid);
                Path pidRefsFilePath =
                    fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);

                assertEquals(cidRefsPath, cidRefsFilePath.toString());
                assertEquals(pidRefsPath, pidRefsFilePath.toString());
            }
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
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream dataStream = Files.newInputStream(testDataFile);
                 InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                // Store Metadata
                fileHashStore.storeMetadata(metadataStream, pid);

                FileHashStore.ObjectInfo findObjInfo = fileHashStore.findObject(pid);
                String objInfoSysmetaPath = findObjInfo.sysmetaPath();

                String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
                Path sysmetaPath =
                    fileHashStore.getHashStoreMetadataPath(pid, storeMetadataNamespace);

                assertEquals(objInfoSysmetaPath, sysmetaPath.toString());
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                FileHashStore.ObjectInfo findObjInfo = fileHashStore.findObject(pid);
                String objInfoSysmetaPath = findObjInfo.sysmetaPath();

                assertEquals(objInfoSysmetaPath, "Does not exist");
            }
        }
    }

    /**
     * Confirm findObject throws exception when cid object does not exist but reference files
     * exist.
     */
    @Test
    public void findObject_refsFileExistButObjectDoesNot() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        assertThrows(OrphanRefsFilesException.class, () -> fileHashStore.findObject(pid));
    }

    /**
     * Confirm that findObject throws OrphanPidRefsFileException exception when pid refs file found
     * but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        Files.delete(cidRefsPath);

        assertThrows(OrphanPidRefsFileException.class, () -> fileHashStore.findObject(pid));
    }


    /**
     * Confirm that findObject throws PidNotFoundInCidRefsFileException exception when pid refs file
     * found but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileMissingPid() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        fileHashStore.updateRefsFile(
            pid, cidRefsPath, FileHashStore.HashStoreRefUpdateTypes.remove);

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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata address =
                    fileHashStore.putObject(dataStream, pid, null, null, null, -1);

                // Check id (sha-256 hex digest of the ab_id, aka object_cid)
                String objContentId = testData.pidData.get(pid).get("sha256");
                assertEquals(objContentId, address.cid());
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.putObject(dataStream, pid, null, null, null, -1);

                // Check id (sha-256 hex digest of the ab_id (pid))
                long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
                assertEquals(objectSize, objInfo.size());
            }

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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata address =
                    fileHashStore.putObject(dataStream, pid, null, null, null, -1);

                Map<String, String> hexDigests = address.hexDigests();

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

        try (InputStream dataStream = Files.newInputStream(testDataFile)) {
            ObjectMetadata address =
                fileHashStore.putObject(dataStream, pid, null, checksumCorrect, "MD2", -1);

            String objCid = address.cid();
            // Get relative path
            String objCidShardString = FileHashStoreUtility.getHierarchicalPathString(3, 2, objCid);
            // Get absolute path
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path objCidAbsPath = storePath.resolve("objects/" + objCidShardString);

            assertTrue(Files.exists(objCidAbsPath));
        }
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

        try (InputStream dataStream = Files.newInputStream(testDataFile)) {
            fileHashStore.putObject(dataStream, pid, "MD2", null, null, -1);

            String md2 = testData.pidData.get(pid).get("md2");
            assertEquals(checksumCorrect, md2);
        }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, null, checksumIncorrect, "MD2", -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, null, "   ", "MD2", -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, null, null, "MD2", -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, null, "abc", "   ", -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, null, "abc", null, -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.putObject(dataStream, pid, null, null, null, objectSize);

                // Check id (sha-256 hex digest of the ab_id (pid))
                assertEquals(objectSize, objInfo.size());
            }
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

                try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                    ObjectMetadata objInfo =
                        fileHashStore.putObject(dataStream, pid, null, null, null, 1000);

                    // Check id (sha-256 hex digest of the ab_id (pid))
                    long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
                    assertEquals(objectSize, objInfo.size());
                }
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

        try (InputStream dataStream = Files.newInputStream(testDataFile)) {
            fileHashStore.putObject(dataStream, pid, null, null, null, -1);
        }


        // Try duplicate upload
        String pidTwo = pid + ".test";
        try (InputStream dataStreamTwo = Files.newInputStream(testDataFile)) {
            fileHashStore.putObject(dataStreamTwo, pidTwo, null, null, null, -1);
        }

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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, "SM2", null, null, -1);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.putObject(dataStream, pid, "   ", null, null, -1);
            }
        });
    }

    /**
     * Confirm validateTmpObject does nothing when requestValidation is false and does not throw any
     * exceptions
     */
    @Test
    public void validateTmpObject() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();
        fileHashStore.validateTmpObject(false, "checksum.string", "SHA-256", tmpFile, hexDigests,
                                        -1);
    }

    /**
     * Confirm validateTmpObject does not throw exception when expected sie matches store size
     */
    @Test
    public void validateTmpObject_sizeMatches() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();

        // Write the byte to the file
        try (FileOutputStream fos = new FileOutputStream(tmpFile)) {
            fos.write(0x41);
        } catch (IOException e) {
            e.printStackTrace();
        }

        fileHashStore.validateTmpObject(false, "sha256Digest", "SHA-256", tmpFile, hexDigests, 1);
    }

    /**
     * Confirm validateTmpObject does not throw exception when expected sie matches store size
     */
    @Test
    public void validateTmpObject_sizeMismatch() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();

        assertThrows(NonMatchingObjSizeException.class,
                     () -> fileHashStore.validateTmpObject(false, "checksum.string", "SHA-256",
                                                           tmpFile, hexDigests, 10));
    }

    /**
     * GG Confirm validateTmpObject does not throw exception when requested to validate checksums
     * with good values
     */
    @Test
    public void validateTmpObject_validationRequested_matchingChecksum() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();
        fileHashStore.validateTmpObject(true, "sha256Digest", "SHA-256", tmpFile, hexDigests, -1);
    }

    /**
     * Confirm validateTmpObject does not throw exception when requested to validate checksums with
     * good values, and that the tmpFile passed is deleted.
     */
    @Test
    public void validateTmpObject_validationRequested_nonMatchingChecksum() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();

        assertThrows(NonMatchingChecksumException.class,
                     () -> fileHashStore.validateTmpObject(true, "checksum.string", "SHA-256",
                                                           tmpFile, hexDigests, -1));
        assertFalse(Files.exists(tmpFile.toPath()));
    }

    /**
     * Confirm validateTmpObject throws exception when requested to validate but algo is not found
     * in hex digests passed.
     */
    @Test
    public void validateTmpObject_validationRequested_algoNotFound() throws Exception {
        Map<String, String> hexDigests = new HashMap<>();
        hexDigests.put("MD5", "md5Digest");
        hexDigests.put("SHA-256", "sha256Digest");
        File tmpFile = generateTemporaryFile();

        assertThrows(NoSuchAlgorithmException.class,
                     () -> fileHashStore.validateTmpObject(true, "md2Digest", "MD2", tmpFile,
                                                           hexDigests, -1));
        assertFalse(Files.exists(tmpFile.toPath()));
    }

    /**
     * Check algorithm support for supported algorithm
     */
    @Test
    public void validateAlgorithm_supported() {
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
    public void validateAlgorithm_notSupported() {
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
    public void validateAlgorithm_notSupportedLowerCase() {
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
    public void validateAlgorithm_algorithmNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            try {
                fileHashStore.validateAlgorithm(null);

            } catch (NoSuchAlgorithmException nsae) {
                fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

            }
        });
    }

    /**
     * Confirm shouldCalculateAlgorithm returns false if algorithm not part of default list
     */
    @Test
    public void shouldCalculateAlgorithm_algoIncluded() {
        boolean shouldCalculate = fileHashStore.shouldCalculateAlgorithm("SHA-256");
        assertFalse(shouldCalculate);
    }

    /**
     * Confirm shouldCalculateAlgorithm returns false if algorithm not part of default list
     */
    @Test
    public void shouldCalculateAlgorithm_algoNotIncluded() {
        boolean shouldCalculate = fileHashStore.shouldCalculateAlgorithm("SHA-DOU");
        assertTrue(shouldCalculate);
    }

    /**
     * Confirm verifyChecksumParameters returns true with good values
     */
    @Test
    public void verifyChecksumParameters() throws Exception {
        boolean shouldValidate = fileHashStore.verifyChecksumParameters("abc123", "SHA-256");
        assertTrue(shouldValidate);
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum value is empty
     */
    @Test
    public void verifyChecksumParameters_emptyChecksum() {
        assertThrows(IllegalArgumentException.class,
                     () -> fileHashStore.verifyChecksumParameters("     ", "SHA-256"));
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum algorithm is empty
     */
    @Test
    public void verifyChecksumParameters_emptyAlgorithm() {
        assertThrows(IllegalArgumentException.class,
                     () -> fileHashStore.verifyChecksumParameters("abc123", "     "));
    }

    /**
     * Confirm verifyChecksumParameters throws exception when checksum algorithm is not supported
     */
    @Test
    public void verifyChecksumParameters_unsupportedAlgorithm() {
        assertThrows(NoSuchAlgorithmException.class,
                     () -> fileHashStore.verifyChecksumParameters("abc123", "SHA-DOU"));
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, null,
                                                                     null);

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

                assertEquals(hexDigests.size(), 5);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.writeToTmpFileAndGenerateChecksums(
                    newTmpFile, dataStream, addAlgo, null);
            }

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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream,
                                                                     addAlgo, null);
                // Validate checksum values
                String md2 = testData.pidData.get(pid).get("md2");
                assertEquals(md2, hexDigests.get("MD2"));

                assertEquals(hexDigests.size(), 6);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, null,
                                                                     checksumAlgo);
                // Validate checksum values
                String sha512224 = testData.pidData.get(pid).get("sha512-224");
                assertEquals(sha512224, hexDigests.get("SHA-512/224"));

                assertEquals(hexDigests.size(), 6);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStore.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream,
                                                                     addAlgo, checksumAlgo);
                // Validate checksum values
                String md2 = testData.pidData.get(pid).get("md2");
                String sha512224 = testData.pidData.get(pid).get("sha512-224");
                assertEquals(md2, hexDigests.get("MD2"));
                assertEquals(sha512224, hexDigests.get("SHA-512/224"));

                assertEquals(hexDigests.size(), 7);
            }
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

                try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                    fileHashStore.writeToTmpFileAndGenerateChecksums(
                        newTmpFile, dataStream, addAlgo, null);
                }
            });
        }
    }

    /**
     * Confirm that object has moved
     */
    @Test
    public void move() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);

        fileHashStore.move(newTmpFile, targetFile, "object");
        assertTrue(targetFile.exists());
    }

    /**
     * Confirm directories have 'rwxr-x---' permissions
     */
    @Test
    public void move_directoryPermissions() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.toString() + "/testmove/subdir1/subdir2/test_tmp_object"
            + ".tmp";
        File targetFile = new File(targetString);

        fileHashStore.move(newTmpFile, targetFile, "object");

        Path path = Paths.get(targetFile.toString());
        while (path.getParent() != null) {
            path = path.getParent();
            // Check if the directory name starts with "testmove"
            if (path.getFileName().toString().startsWith("junit")) {
                break;
            } else {
                System.out.println(path);
                Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(path);

                assertTrue(actualPermissions.contains(PosixFilePermission.OWNER_READ));
                assertTrue(actualPermissions.contains(PosixFilePermission.OWNER_WRITE));
                assertTrue(actualPermissions.contains(PosixFilePermission.OWNER_EXECUTE));
                assertTrue(actualPermissions.contains(PosixFilePermission.GROUP_READ));
                assertFalse(actualPermissions.contains(PosixFilePermission.GROUP_WRITE));
                assertTrue(actualPermissions.contains(PosixFilePermission.GROUP_EXECUTE));
                assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_READ));
                assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_WRITE));
                assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_EXECUTE));
            }
        }
    }

    /**
     * Confirm file has 'rw-r-----' permissions
     */
    @Test
    public void move_filePermissions() throws Exception {
        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.toString() + "/testmove/subdir1/subdir2/test_tmp_object"
            + ".tmp";
        File targetFile = new File(targetString);

        fileHashStore.move(newTmpFile, targetFile, "object");

        Set<PosixFilePermission> actualPermissions =
            Files.getPosixFilePermissions(targetFile.toPath());

        assertTrue(actualPermissions.contains(PosixFilePermission.OWNER_READ));
        assertTrue(actualPermissions.contains(PosixFilePermission.OWNER_WRITE));
        assertFalse(actualPermissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertTrue(actualPermissions.contains(PosixFilePermission.GROUP_READ));
        assertFalse(actualPermissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(actualPermissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_EXECUTE));
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
     * Confirm deleteObjectByCid method deletes object when there are no references.
     */
    @Test
    public void deleteObjectByCid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                // Store object only
                ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
                String cid = objInfo.cid();

                // Try deleting the object
                fileHashStore.deleteObjectByCid(cid);

                // Get permanent address of the actual cid
                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
                String objShardString =
                    FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);

                Path objRealPath = storePath.resolve("objects").resolve(objShardString);
                assertFalse(Files.exists(objRealPath));
            }
        }
    }

    /**
     * Confirm deleteObjectByCid method does not delete an object if a cid refs file exists (pids
     * still referencing the cid).
     */
    @Test
    public void deleteObjectByCid_cidRefsFileContainsPids() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
                String cid = objInfo.cid();

                // Try deleting the object
                fileHashStore.deleteObjectByCid(cid);

                // Get permanent address of the actual cid
                Path objRealPath = fileHashStore.getHashStoreDataObjectPath(pid);
                assertTrue(Files.exists(objRealPath));
            }
        }
    }

    /**
     * Check that storeHashStoreRefsFiles creates reference files
     */
    @Test
    public void storeHashStoreRefsFiles() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        // Confirm refs files exist
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);

        assertTrue(Files.exists(absCidRefsPath));
        assertTrue(Files.exists(absPidRefsPath));

        // Confirm no additional files were created
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that storeHashStoreRefsFiles writes expected pid refs files and that the content is
     * correct
     */
    @Test
    public void storeHashStoreRefsFiles_pidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        Path pidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
        assertTrue(Files.exists(pidRefsFilePath));

        String retrievedCid = new String(Files.readAllBytes(pidRefsFilePath));
        assertEquals(cid, retrievedCid);
    }

    /**
     * Check that storeHashStoreRefsFiles writes expected cid refs files and that the content is
     * correct
     */
    @Test
    public void storeHashStoreRefsFiles_cidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        assertTrue(Files.exists(cidRefsFilePath));

        String retrievedPid = new String(Files.readAllBytes(cidRefsFilePath));
        assertEquals(pid, retrievedPid);
    }

    /**
     * Check that storeHashStoreRefsFiles throws HashStoreRefsAlreadyExistException when refs files
     * already exist
     */
    @Test
    public void storeHashStoreRefsFiles_HashStoreRefsAlreadyExistException() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        assertThrows(HashStoreRefsAlreadyExistException.class,
                     () -> fileHashStore.storeHashStoreRefsFiles(pid, cid));

        // Confirm that there is only 1 of each ref file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check storeHashStoreRefsFiles throws exception when the supplied cid is different from what
     * is found in the pid refs file, and the associated cid refs file from the pid refs file is
     * correctly tagged (everything is where it's expected to be)
     */
    @Test
    public void storeHashStoreRefsFiles_PidRefsFileExistsException() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String existingCid = "987654321fedcba";
        fileHashStore.storeHashStoreRefsFiles(pid, existingCid);

        // This will throw an exception because the pid and cid refs file are in sync
        assertThrows(PidRefsFileExistsException.class,
                     () -> fileHashStore.storeHashStoreRefsFiles(pid, cid));
    }

    /**
     * Check storeHashStoreRefsFiles throws exception when a pid refs file already exists
     */
    @Test
    public void storeHashStoreRefsFiles_pidRefsOrphanedFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String cidForOrphanPidRef = "987654321fedcba";

        // Create orphaned pid refs file
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidForOrphanPidRef,
                                                          FileHashStore.HashStoreIdTypes.pid.name());
        File absPathPidRefsFile = absPidRefsPath.toFile();
        fileHashStore.move(pidRefsTmpFile, absPathPidRefsFile, "refs");

        assertThrows(PidRefsFileExistsException.class,
                     () -> fileHashStore.storeHashStoreRefsFiles(pid, cid));
    }

    /**
     * Check that storeHashStoreRefsFiles creates a pid refs file and updates an existing cid refs
     * file
     */
    @Test
    public void storeHashStoreRefsFiles_updateExistingRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        String pidAdditional = "another.pid.2";
        fileHashStore.storeHashStoreRefsFiles(pidAdditional, cid);

        // Confirm missing pid refs file has been created
        Path pidAdditionalRefsFilePath =
            fileHashStore.getHashStoreRefsPath(pidAdditional, FileHashStore.HashStoreIdTypes.pid);
        assertTrue(Files.exists(pidAdditionalRefsFilePath));

        // Check cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        boolean pidFoundInCidRefFiles =
            fileHashStore.isStringInRefsFile(pidAdditional, cidRefsFilePath);
        assertTrue(pidFoundInCidRefFiles);

        // There should be 2 pid refs file, and 1 cid refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(2, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that unTagObject deletes reference files
     */
    @Test
    public void unTagObject() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                String cid = objInfo.cid();
                fileHashStore.synchronizeReferenceLockedPids(pid);
                fileHashStore.synchronizeObjectLockedCids(cid);

                fileHashStore.unTagObject(pid, cid);

                fileHashStore.releaseReferenceLockedPids(pid);
                fileHashStore.releaseObjectLockedCids(cid);

                // Confirm refs files do not exist
                Path absCidRefsPath =
                    fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
                Path absPidRefsPath =
                    fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
                assertFalse(Files.exists(absCidRefsPath));
                assertFalse(Files.exists(absPidRefsPath));
            }
        }
    }

    /**
     * Confirm IdentifierNotLockedException is thrown when pid is not locked
     */
    @Test
    public void unTagObject_pid_IdentifierNotLockedException() {
        for (String pid : testData.pidList) {
            assertThrows(
                IdentifierNotLockedException.class, () -> fileHashStore.unTagObject(pid, "cid"));
        }
    }

    /**
     * Confirm IdentifierNotLockedException is thrown when cid is not locked
     */
    @Test
    public void unTagObject_cid_IdentifierNotLockedException() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            // The object must be stored otherwise the unTag process cannot execute as expected
            ObjectMetadata objInfo = null;
            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            }
            String cid = objInfo.cid();

            FileHashStore.synchronizeReferenceLockedPids(pid);
            assertThrows(IdentifierNotLockedException.class,
                         () -> fileHashStore.unTagObject(pid, cid));
            FileHashStore.releaseReferenceLockedPids(pid);
        }
    }

    /**
     * Confirm IllegalArgumentException is thrown when cid retrieved does not match what has been
     * provided.
     */
    @Test
    public void unTagObject_cid_doesNotMatchFindObject() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            // The object must be stored otherwise the unTag process cannot execute as expected
            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            }

            fileHashStore.synchronizeReferenceLockedPids(pid);
            fileHashStore.synchronizeObjectLockedCids("does_not_match");
            assertThrows(
                IllegalArgumentException.class, () -> fileHashStore.unTagObject(pid, "cid"));
            fileHashStore.releaseReferenceLockedPids(pid);
            fileHashStore.releaseObjectLockedCids("does_not_match");
        }
    }

    /**
     * Check that unTagObject deletes pid refs file for a cid that is referenced by multiple pids,
     * and that the cid refs file is not deleted.
     */
    @Test
    public void unTagObject_cidWithMultiplePidReferences() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);
        String cid = testData.pidData.get(pid).get("sha256");

        Collection<String> pidList = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            pidList.add(pid + "." + i);
        }

        // The object must be stored otherwise the unTag process cannot execute as expected
        for (String pidToUse : pidList) {
            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pidToUse, null, null, null, -1);
            }
        }

        String pidToCheck = pid + ".1";

        fileHashStore.synchronizeReferenceLockedPids(pidToCheck);
        fileHashStore.synchronizeObjectLockedCids(cid);

        fileHashStore.unTagObject(pidToCheck, cid);

        fileHashStore.releaseReferenceLockedPids(pidToCheck);
        fileHashStore.releaseObjectLockedCids(cid);

        // Confirm refs files state
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pidToCheck, FileHashStore.HashStoreIdTypes.pid);

        assertFalse(Files.exists(absPidRefsPath));
        assertTrue(Files.exists(absCidRefsPath));

        // Confirm number of reference files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(3, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that unTagObject deletes an orphaned pid refs file (there is no cid refs file)
     */
    @Test
    public void unTagObject_orphanPidRefsFile() throws Exception {

        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                String cid = objInfo.cid();
                // Delete cid refs file to create orphaned pid refs file
                Path absCidRefsPath =
                    fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
                Files.delete(absCidRefsPath);
                assertFalse(Files.exists(absCidRefsPath));

                fileHashStore.synchronizeReferenceLockedPids(pid);
                fileHashStore.synchronizeObjectLockedCids(cid);

                fileHashStore.unTagObject(pid, cid);

                fileHashStore.releaseReferenceLockedPids(pid);
                fileHashStore.releaseObjectLockedCids(cid);

                // Confirm pid refs is deleted
                Path absPidRefsPath =
                    fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
                assertFalse(Files.exists(absPidRefsPath));

                // Confirm number of reference files
                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                List<Path> pidRefsFiles =
                    FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
                List<Path> cidRefsFiles =
                    FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

                assertEquals(0, pidRefsFiles.size());
                assertEquals(0, cidRefsFiles.size());
            }
        }
    }

    /**
     * Check that unTagObject does not throw exception when a pid refs file and cid refs file does
     * not exist
     */
    @Test
    public void unTagObject_missingRefsFiles() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                // Store data object only
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream);

                String cid = objInfo.cid();
                fileHashStore.synchronizeReferenceLockedPids(pid);
                fileHashStore.synchronizeObjectLockedCids(cid);

                fileHashStore.unTagObject(pid, cid);

                fileHashStore.releaseReferenceLockedPids(pid);
                fileHashStore.releaseObjectLockedCids(cid);
            }
        }
    }

    /**
     * Check that unTagObject successfully removes a pid from a cid refs file when a pid refs file
     * is missing but the pid is referenced in a cid refs file
     */
    @Test
    public void unTagObject_missingPidRefsFile() throws Exception {
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);
        String cid = testData.pidData.get(pid).get("sha256");

        Collection<String> pidList = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            pidList.add(pid + "." + i);
        }

        // The object must be stored otherwise the unTag process cannot execute as expected
        for (String pidToUse : pidList) {
            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pidToUse, null, null, null, -1);
            }
        }

        String pidToCheck = pid + ".1";

        // Delete pid refs to create scenario
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pidToCheck, FileHashStore.HashStoreIdTypes.pid);
        Files.delete(absPidRefsPath);
        assertFalse(Files.exists(absPidRefsPath));

        fileHashStore.synchronizeReferenceLockedPids(pidToCheck);
        fileHashStore.synchronizeObjectLockedCids(cid);

        fileHashStore.unTagObject(pidToCheck, cid);

        fileHashStore.releaseReferenceLockedPids(pidToCheck);
        fileHashStore.releaseObjectLockedCids(cid);

        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        assertFalse(fileHashStore.isStringInRefsFile(pidToCheck, absCidRefsPath));
    }

    /**
     * Check that unTagObject successfully deletes cid and pid refs file when a data object does
     * not exist.
     */
    @Test
    public void unTagObject_refsExistButDataObjectDoesNotExist() throws Exception {
        String pid = "jtao.1700.1";
        String cid = testData.pidData.get(pid).get("sha256");

        Collection<String> pidList = new ArrayList<>();
        for (int i = 1; i < 5; i++) {
            pidList.add(pid + "." + i);
        }

        // The object must be stored otherwise the unTag process cannot execute as expected
        for (String pidToUse : pidList) {
            fileHashStore.tagObject(pidToUse, cid);
        }

        String pidToCheck = pid + ".1";

        fileHashStore.synchronizeReferenceLockedPids(pidToCheck);
        fileHashStore.synchronizeObjectLockedCids(cid);

        fileHashStore.unTagObject(pidToCheck, cid);

        fileHashStore.releaseReferenceLockedPids(pidToCheck);
        fileHashStore.releaseObjectLockedCids(cid);

        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        assertFalse(fileHashStore.isStringInRefsFile(pidToCheck, absCidRefsPath));
    }

    /**
     * Check that unTagObject successfully deletes cid and pid refs file when a data object does
     * not exist.
     */
    @Test
    public void unTagObject_refsExistNoObject_singlePidInCidRefs() throws Exception {
        String pid = "jtao.1700.1";
        String cid = testData.pidData.get(pid).get("sha256");

        fileHashStore.tagObject(pid, cid);


        fileHashStore.synchronizeReferenceLockedPids(pid);
        fileHashStore.synchronizeObjectLockedCids(cid);

        fileHashStore.unTagObject(pid, cid);

        fileHashStore.releaseReferenceLockedPids(pid);
        fileHashStore.releaseObjectLockedCids(cid);

        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        assertFalse(Files.exists(absPidRefsPath));
        assertFalse(Files.exists(absCidRefsPath));
    }

    /**
     * Check that no exception is thrown when pid and cid are tagged correctly
     */
    @Test
    public void verifyHashStoreRefFiles() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Create a pid refs file with the incorrect cid
        Path pidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath, cidRefsFilePath);
    }

    /**
     * Check that an exception is thrown when a file is not found
     */
    @Test
    public void verifyHashStoreRefFiles_fileNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";

        // Create a pid refs file with the incorrect cid
        Path pidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        assertThrows(FileNotFoundException.class,
                     () -> fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath,
                                                                  cidRefsFilePath));
    }

    /**
     * Check that exception is thrown when incorrect cid in a pid refs file.
     */
    @Test
    public void verifyHashStoreRefFiles_unexpectedCid() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Create a pid refs file with the incorrect cid
        String cidToWrite = "123456789abcdef";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");
        Path pidRefsTmpFilePath = pidRefsTmpFile.toPath();

        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        assertThrows(CidNotFoundInPidRefsFileException.class,
                     () -> fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsTmpFilePath,
                                                                  cidRefsFilePath));
    }

    /**
     * Check that exception is thrown when an expected pid is not found in a cid refs file
     */
    @Test
    public void verifyHashStoreRefFiles_pidNotFoundInCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Create a cid refs file with a different pid from the one that is expected
        String pidToWrite = "dou.test.2";
        File cidRefsTmpFile = fileHashStore.writeRefsFile(pidToWrite, "cid");
        Path cidRefsTmpFilePath = cidRefsTmpFile.toPath();

        // Get path of the pid refs file
        Path pidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);

        assertThrows(PidNotFoundInCidRefsFileException.class,
                     () -> fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath,
                                                                  cidRefsTmpFilePath));
    }

    /**
     * Check that the value supplied is written
     */
    @Test
    public void writeRefsFile_content() throws Exception {
        String cidToWrite = "test_cid_123";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");

        String cidRead = new String(Files.readAllBytes(pidRefsTmpFile.toPath()));
        assertEquals(cidRead, cidToWrite);
    }

    /**
     * Check isStringInRefsFile returns true when value is found
     */
    @Test
    public void isStringInRefsFile_found() throws Exception {
        String cidToWrite = "test_cid_123";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");

        assertTrue(fileHashStore.isStringInRefsFile(cidToWrite, pidRefsTmpFile.toPath()));
    }

    /**
     * Check isStringInRefsFile returns false when value is not found
     */
    @Test
    public void isStringInRefsFile_notFound() throws Exception {
        String cidToWrite = "test_cid_123";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");

        assertFalse(fileHashStore.isStringInRefsFile("not.found.in.ref", pidRefsTmpFile.toPath()));
    }

    /**
     * Check isStringInRefsFile returns true when value is found in a refs file with multiple values
     * and returns false when a value isn't found
     */
    @Test
    public void isStringInRefsFile_cidRefsMultipleVals() throws Exception {
        String cid = "abcdef123456789";
        fileHashStore.tagObject("dou.test.1", cid);
        fileHashStore.tagObject("dou.test.2", cid);
        fileHashStore.tagObject("dou.test.3", cid);
        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        assertTrue(fileHashStore.isStringInRefsFile("dou.test.1", cidRefsFilePath));
        assertFalse(fileHashStore.isStringInRefsFile("wont.be.found", cidRefsFilePath));
    }

    /**
     * Confirm that cid refs file has been updated successfully
     */
    @Test
    public void updateRefsFile_add() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        String pidAdditional = "dou.test.2";
        fileHashStore.updateRefsFile(
            pidAdditional, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.add);

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        boolean pidAdditional_foundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(pidAdditional)) {
                pidAdditional_foundInCidRefFiles = true;
            }
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertTrue(pidOriginal_foundInCidRefFiles);
        assertTrue(pidAdditional_foundInCidRefFiles);
    }

    /**
     * Confirm that cid refs file does not add duplicate value
     */
    @Test
    public void updateRefsFile_addDuplicateValue() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        String pidAdditional = "dou.test.2";
        fileHashStore.updateRefsFile(
            pidAdditional, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.add);
        // Try re-adding it
        fileHashStore.updateRefsFile(
            pidAdditional, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.add);

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        assertEquals(lines.size(), 2);
    }

    /**
     * Check that updateRefsFile removes pid from its cid refs file
     */
    @Test
    public void updateRefsFile_remove() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        String pidAdditional = "dou.test.2";
        fileHashStore.tagObject(pidAdditional, cid);

        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        fileHashStore.updateRefsFile(
            pid, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);

        assertFalse(fileHashStore.isStringInRefsFile(pid, cidRefsFilePath));
    }

    /**
     * Check that updateRefsFile removes all pids as expected and leaves an empty file.
     */
    @Test
    public void updateRefsFile_removeMultiplePids() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        String pidAdditional = "dou.test.2";
        fileHashStore.tagObject(pidAdditional, cid);
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

        fileHashStore.updateRefsFile(
            pid, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);
        fileHashStore.updateRefsFile(
            pidAdditional, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);

        assertTrue(Files.exists(cidRefsFilePath));
        assertEquals(0, Files.size(cidRefsFilePath));
    }

    /**
     * Confirm that updateRefsFile does not throw any exception if called to remove a value that is
     * not found in a cid refs file.
     */
    @Test
    public void updateRefsFile_cidRefsPidNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        fileHashStore.updateRefsFile(
            "dou.test.2", cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        int pidsFound = 0;
        for (String line : lines) {
            pidsFound++;
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertTrue(pidOriginal_foundInCidRefFiles);
        assertEquals(1, pidsFound);
    }

    /**
     * Confirm that updateRefsFile does not throw any exception if called to remove a value from a
     * cid refs file that is empty
     */
    @Test
    public void updateRefsFile_cidRefsEmpty() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath =
            fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
        fileHashStore.updateRefsFile(
            pid, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        int pidsFound = 0;
        for (String line : lines) {
            pidsFound++;
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertFalse(pidOriginal_foundInCidRefFiles);
        assertEquals(0, pidsFound);

        // Confirm that no exception is thrown and that the cid refs still exists
        fileHashStore.updateRefsFile(
            pid, cidRefsFilePath, FileHashStore.HashStoreRefUpdateTypes.remove);
        assertTrue(Files.exists(cidRefsFilePath));
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

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                String metadataPath = fileHashStore.putMetadata(metadataStream, pid, null);

                // Calculate absolute path
                String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
                Path metadataPidExpectedPath =
                    fileHashStore.getHashStoreMetadataPath(pid, storeMetadataNamespace);
                assertEquals(metadataPath, metadataPidExpectedPath.toString());
            }
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

                try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                    fileHashStore.putMetadata(metadataStream, null, null);
                }
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

                try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                    fileHashStore.putMetadata(metadataStream, "", null);
                }
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

                try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                    fileHashStore.putMetadata(metadataStream, "     ", null);
                }
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

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                boolean metadataWritten =
                    fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);
                assertTrue(metadataWritten);
            }
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

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                boolean metadataWritten =
                    fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);
                assertTrue(metadataWritten);

                long tmpMetadataFileSize = Files.size(newTmpFile.toPath());
                long testMetadataFileSize = Files.size(testMetaDataFile);
                assertTrue(tmpMetadataFileSize > 0);
                assertEquals(tmpMetadataFileSize, testMetadataFileSize);
            }
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

            // Create InputStream to tmp File
            try (InputStream metadataStoredStream = Files.newInputStream(newTmpFile.toPath());
                 InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                // Write it to the tmpFile

                fileHashStore.writeToTmpMetadataFile(newTmpFile, metadataStream);
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

                String sha256Digest =
                    DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
                String sha256MetadataDigestFromTestData =
                    testData.pidData.get(pid).get("metadata_cid_sha256");
                assertEquals(sha256Digest, sha256MetadataDigestFromTestData);
            }
        }
    }

    /**
     * Confirm that syncRenameMetadataDocForDeletion adds '_delete' to the given paths
     */
    @Test
    public void syncRenameMetadataDocForDeletion_renamesAsExpected() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                String pathToMetadata = fileHashStore.putMetadata(metadataStream, pid, null);
                String pathToMetadataTwo =
                    fileHashStore.putMetadata(metadataStream, pid, "ns.test.1");
                String pathToMetadataThree =
                    fileHashStore.putMetadata(metadataStream, pid, "ns.test" + ".3");

                // Confirm that metadata documents are present
                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                String storeAlgo = fhsProperties.getProperty("storeAlgorithm");
                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
                String pidHexDigest = FileHashStoreUtility.getPidHexDigest(pid, storeAlgo);
                String pidRelativePath =
                    FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth,
                                                                   pidHexDigest);
                Path expectedPidMetadataDirectory =
                    storePath.resolve("metadata").resolve(pidRelativePath);
                List<Path> metadataDocPaths =
                    FileHashStoreUtility.getFilesFromDir(expectedPidMetadataDirectory);

                assertEquals(3, metadataDocPaths.size());

                Collection<Path> deleteList =
                    fileHashStore.syncRenameMetadataDocForDeletion(metadataDocPaths);

                Collection<String> renamedDocStrings = new ArrayList<>();
                for (Path renamedDoc : deleteList) {
                    renamedDocStrings.add(renamedDoc.toString());
                }
                assertTrue(renamedDocStrings.contains(pathToMetadata + "_delete"));
                assertTrue(renamedDocStrings.contains(pathToMetadataTwo + "_delete"));
                assertTrue(renamedDocStrings.contains(pathToMetadataThree + "_delete"));
            }
        }
    }

    /**
     * Confirm that syncRenameMetadataDocForDeletion throws exception when supplied list is empty.
     */
    @Test
    public void syncRenameMetadataDocForDeletion_emptyList() {
        Collection<Path> metadataDocPaths = new ArrayList<>();
        assertThrows(IllegalArgumentException.class,
                     () -> fileHashStore.syncRenameMetadataDocForDeletion(metadataDocPaths));
    }

    /**
     * Confirm that syncRenameMetadataDocForDeletion throws exception when supplied list is null.
     */
    @Test
    public void syncRenameMetadataDocForDeletion_nullList() {
        assertThrows(IllegalArgumentException.class,
                     () -> fileHashStore.syncRenameMetadataDocForDeletion(null));
    }

    /**
     * Confirm that isStringInRefsFile returns true when pid is found
     */
    @Test
    public void isStringInRefsFile_pidFound() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            }

            String pidTwo = pid + ".test";

            try (InputStream dataStreamDup = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStreamDup, pidTwo, null, null, null, -1);

                String cid = objInfo.cid();
                Path absCidRefsPath =
                    fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
                assertTrue(fileHashStore.isStringInRefsFile(pidTwo, absCidRefsPath));
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                String cid = objInfo.cid();
                Path absCidRefsPath =
                    fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);
                assertFalse(fileHashStore.isStringInRefsFile("pid.not.found", absCidRefsPath));
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
                String cid = objInfo.cid();

                // Manually form the permanent address of the actual cid
                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
                String objShardString =
                    FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);
                Path calculatedObjRealPath = storePath.resolve("objects").resolve(objShardString);

                Path expectedObjCidAbsPath = fileHashStore.getHashStoreDataObjectPath(pid);

                assertEquals(expectedObjCidAbsPath, calculatedObjRealPath);
            }
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

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                fileHashStore.storeMetadata(metadataStream, pid);

                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                String storeFormatId = fhsProperties.getProperty("storeMetadataNamespace");
                String storeAlgo = fhsProperties.getProperty("storeAlgorithm");
                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));

                // Document ID
                String hashId =
                    FileHashStoreUtility.getPidHexDigest(pid + storeFormatId, storeAlgo);

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

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                fileHashStore.storeMetadata(metadataStream, pid, null);

                String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");

                InputStream metadataCidInputStream =
                    fileHashStore.getHashStoreMetadataInputStream(pid, storeFormatId);
                assertNotNull(metadataCidInputStream);
            }
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

            assertThrows(FileNotFoundException.class,
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

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

                Path expectedPidRefsPath =
                    fileHashStore.getHashStoreRefsPath(pid, FileHashStore.HashStoreIdTypes.pid);

                assertEquals(expectedPidRefsPath, calculatedPidRefsRealPath);
            }
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

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                ObjectMetadata objInfo =
                    fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
                String cid = objInfo.cid();

                // Manually form the permanent address of the actual cid
                Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
                int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
                int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));

                // Cid refs file
                String objShardString =
                    FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth, cid);
                Path calculatedCidRefsRealPath =
                    storePath.resolve("refs/cids").resolve(objShardString);

                Path expectedCidRefsPath =
                    fileHashStore.getHashStoreRefsPath(cid, FileHashStore.HashStoreIdTypes.cid);

                assertEquals(expectedCidRefsPath, calculatedCidRefsRealPath);
            }
        }
    }

    /**
     * Confirm getHashStoreDataObjectPath throws exception when requesting path for an object that
     * does not exist
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
    public void fileHashStoreUtility_checkForEmptyAndValidString() {
        assertThrows(IllegalArgumentException.class,
                     () -> FileHashStoreUtility.checkForNotEmptyAndValidString("dou.test.1\n",
                                                                               "pid"));
    }

    /**
     * Confirm getExpectedPath throws exception when requesting path for an object that does not
     * exist
     */
    @Test
    public void fileHashStoreUtility_checkForEmptyAndValidString_newLine() {
        assertThrows(IllegalArgumentException.class,
                     () -> FileHashStoreUtility.checkForNotEmptyAndValidString("\n", "pid"));
    }

    /**
     * Confirm that renamePathForDeletion adds '_delete' to the given path
     */
    @Test
    public void fileHashStoreUtility_renamePathForDeletion() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                String pathToMetadata = fileHashStore.putMetadata(metadataStream, pid, null);

                Path metadataPath = Paths.get(pathToMetadata);
                FileHashStoreUtility.renamePathForDeletion(metadataPath);

                Path expectedMetadataPathRenamed = Paths.get(pathToMetadata + "_delete");
                assertTrue(Files.exists(expectedMetadataPathRenamed));
            }
        }
    }

    /**
     * Confirm that renamePathForDeletion adds '_delete' to the given path
     */
    @Test
    public void fileHashStoreUtility_renamePathForRestoration() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                String pathToMetadata = fileHashStore.putMetadata(metadataStream, pid, null);

                Path metadataPath = Paths.get(pathToMetadata);
                FileHashStoreUtility.renamePathForDeletion(metadataPath);

                Path expectedMetadataPathRenamed = Paths.get(pathToMetadata + "_delete");
                assertFalse(Files.exists(metadataPath));
                assertTrue(Files.exists(expectedMetadataPathRenamed));

                FileHashStoreUtility.renamePathForRestoration(expectedMetadataPathRenamed);
                assertFalse(Files.exists(expectedMetadataPathRenamed));
                assertTrue(Files.exists(metadataPath));
            }
        }
    }


    /**
     * Confirm that generateTemporaryFile creates tmpFile with expected permissions
     */
    @Test
    public void fileHashStoreUtility_generateTmpFile_permissions() throws Exception {
        Path directory = tempFolder.resolve("hashstore");
        // newFile
        File tmpFile = FileHashStoreUtility.generateTmpFile("testfile", directory);

        Collection<PosixFilePermission> expectedPermissions = new HashSet<>();
        expectedPermissions.add(PosixFilePermission.OWNER_READ);
        expectedPermissions.add(PosixFilePermission.OWNER_WRITE);
        expectedPermissions.add(PosixFilePermission.GROUP_READ);

        Set<PosixFilePermission> actualPermissions =
            Files.getPosixFilePermissions(tmpFile.toPath());

        assertEquals(expectedPermissions, actualPermissions);
        assertFalse(actualPermissions.contains(PosixFilePermission.OWNER_EXECUTE));
        assertFalse(actualPermissions.contains(PosixFilePermission.GROUP_WRITE));
        assertFalse(actualPermissions.contains(PosixFilePermission.GROUP_EXECUTE));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_READ));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_WRITE));
        assertFalse(actualPermissions.contains(PosixFilePermission.OTHERS_EXECUTE));
    }
}

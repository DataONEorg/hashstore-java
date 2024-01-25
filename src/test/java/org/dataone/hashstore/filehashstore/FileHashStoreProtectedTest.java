package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.ObjectMetadata;
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
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize each FileHashStore test with a new root temporary folder
     */
    @BeforeEach
    public void initializeFileHashStore() {
        Path rootDirectory = tempFolder.resolve("metacat");

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
        Path directory = tempFolder.resolve("metacat");
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
                    "NoSuchAlgorithmException encountered: " + nsae.getMessage()
                );

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
                    "NoSuchAlgorithmException encountered: " + nsae.getMessage()
                );

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
        String shardedPath = FileHashStoreUtility.getHierarchicalPathString(
            3, 2, "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a"
        );
        String shardedPathExpected =
            "94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
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
        ObjectMetadata address = fileHashStore.putObject(
            dataStream, pid, null, checksumCorrect, "MD2", -1
        );

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
            ObjectMetadata objInfo = fileHashStore.putObject(
                dataStream, pid, null, null, null, objectSize
            );

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
                ObjectMetadata objInfo = fileHashStore.putObject(
                    dataStream, pid, null, null, null, 1000
                );

                // Check id (sha-256 hex digest of the ab_id (pid))
                long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
                assertEquals(objectSize, objInfo.getSize());
            });
        }
    }

    /**
     * Verify putObject deletes temporary file written if called to store an object
     * that already exists (duplicate)
     */
    @Test
    public void putObject_duplicateObject() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStream, pid, null, null, null, -1);

        // Try duplicate upload
        String pidTwo = pid + ".test";
        InputStream dataStreamTwo = Files.newInputStream(testDataFile);
        fileHashStore.putObject(dataStreamTwo, pidTwo, null, null, null, -1);

        // Confirm there are no files in 'objects/tmp' directory
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] files = storePath.resolve("objects/tmp").toFile().listFiles();
        assertEquals(0, files.length);
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
                    newTmpFile, dataStream, addAlgo, null
                );
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
    public void testMove_targetExists() throws Exception {
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
    public void testMove_entityNull() {
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
    public void testMove_entityEmpty() {
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
    public void testMove_entityEmptySpaces() {
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
            String metadataCid = fileHashStore.putMetadata(metadataStream, pid, null);

            // Get relative path
            String metadataCidShardString = FileHashStoreUtility.getHierarchicalPathString(
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
    @Test
    public void putMetadata_metadataNull() {
        for (String pid : testData.pidList) {
            assertThrows(
                IllegalArgumentException.class, () -> fileHashStore.putMetadata(null, pid, null)
            );
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

            String pidTwo = pid + ".test";
            InputStream dataStreamDup = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStreamDup, pidTwo, null, null, null, -1
            );

            String cid = objInfo.getCid();
            Path absCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
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
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );

            String cid = objInfo.getCid();
            Path absCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            assertFalse(fileHashStore.isStringInRefsFile("pid.not.found", absCidRefsPath));
        }
    }

    /**
     * Confirm tryDeleteCidObject overload method does not delete an object if pid and cid
     * refs files exist.
     */
    @Test
    public void tryDeleteCidObject_pidRefsExists() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            // Store object only
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            String cid = objInfo.getCid();

            // Set flag to true
            fileHashStore.tryDeleteCidObject(cid);

            // Get permanent address of the actual cid
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String objShardString = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, cid
            );

            Path objRealPath = storePath.resolve("objects").resolve(objShardString);
            assertFalse(Files.exists(objRealPath));
        }
    }

    /**
     * Confirm tryDeleteCidObject overload method does not delete an object if a cid refs file
     * exists (pids still referencing it).
     */
    @Test
    public void tryDeleteCidObject_cidRefsFileContainsPids() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            String cid = objInfo.getCid();

            // Set flag to true
            fileHashStore.tryDeleteCidObject(cid);

            // Get permanent address of the actual cid
            Path objRealPath = fileHashStore.getExpectedPath(pid, "object", null);
            assertTrue(Files.exists(objRealPath));
        }
    }

    @Test
    public void getExpectedPath() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        InputStream dataStream = Files.newInputStream(testDataFile);
        ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
        String cid = objInfo.getCid();

        Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
        Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
        Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        assertTrue(Files.exists(objCidAbsPath));
        assertTrue(Files.exists(pidRefsPath));
        assertTrue(Files.exists(cidRefsPath));
    }
}

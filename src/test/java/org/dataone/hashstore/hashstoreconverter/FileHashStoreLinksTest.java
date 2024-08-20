package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import org.dataone.hashstore.testdata.TestDataHarness;

/**
 * Test class for 'FileHashStoreLinks'
 */
public class FileHashStoreLinksTest {
    private static Path rootDirectory;
    private static Path objStringFull;
    private static Path objTmpStringFull;
    private static Path metadataStringFull;
    private static Path metadataTmpStringFull;
    private static Path refStringFull;
    private static Path refPidsStringFull;
    private static Path refCidsStringFull;
    private static final TestDataHarness testData = new TestDataHarness();
    private FileHashStoreLinks fileHashStoreLinks;

    /**
     * Initialize FileHashStore
     */
    @BeforeEach
    public void initializeFileHashStoreLinks() {
        Path root = tempFolder;
        rootDirectory = root.resolve("hashstore");
        objStringFull = rootDirectory.resolve("objects");
        objTmpStringFull = rootDirectory.resolve("objects/tmp");
        metadataStringFull = rootDirectory.resolve("metadata");
        metadataTmpStringFull = rootDirectory.resolve("metadata/tmp");
        refStringFull = rootDirectory.resolve("refs");
        refPidsStringFull = rootDirectory.resolve("refs/pids");
        refCidsStringFull = rootDirectory.resolve("refs/cids");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

        try {
            fileHashStoreLinks = new FileHashStoreLinks(storeProperties);

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException encountered: " + e.getMessage());

        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

        }
    }

    /**
     * Temporary folder for tests to run in
     */
    @TempDir
    public Path tempFolder;

    /**
     * Check object store and tmp directories are created after initialization
     */
    @Test
    public void initObjDirectories() {
        Path checkObjectStorePath = objStringFull;
        assertTrue(Files.isDirectory(checkObjectStorePath));
        Path checkTmpPath = objTmpStringFull;
        assertTrue(Files.isDirectory(checkTmpPath));
    }

    /**
     * Check metadata store and tmp directories are created after initialization
     */
    @Test
    public void initMetadataDirectories() {
        Path checkMetadataStorePath = metadataStringFull;
        assertTrue(Files.isDirectory(checkMetadataStorePath));
        Path checkMetadataTmpPath = metadataTmpStringFull;
        assertTrue(Files.isDirectory(checkMetadataTmpPath));
    }

    /**
     * Check refs tmp, pid and cid directories are created after initialization
     */
    @Test
    public void initRefsDirectories() {
        assertTrue(Files.isDirectory(refStringFull));
        assertTrue(Files.isDirectory(refPidsStringFull));
        assertTrue(Files.isDirectory(refCidsStringFull));
        Path refsTmpPath = rootDirectory.resolve("refs/tmp");
        assertTrue(Files.isDirectory(refsTmpPath));
    }

    /**
     * Test FileHashStore instantiates with matching config
     */
    @Test
    public void testExistingHashStoreConfiguration_sameConfig() throws Exception {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

        new FileHashStore(storeProperties);
    }

    /**
     * Check that storeHardLink creates hard link and returns the correct ObjectMetadata cid
     */
    @Test
    public void storeHardLink() throws Exception {
        for (String pid : testData.pidList) {
            String sha256 = testData.pidData.get(pid).get("sha256");
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            assertTrue(Files.exists(testDataFile));

            ObjectMetadata objInfo =
                fileHashStoreLinks.storeHardLink(testDataFile, pid, sha256, "SHA-256");

            // Check id (content identifier based on the store algorithm)
            String objectCid = testData.pidData.get(pid).get("sha256");
            assertEquals(objectCid, objInfo.cid());
            assertEquals(pid, objInfo.pid());

            Path objPath = fileHashStoreLinks.getHashStoreLinksDataObjectPath(pid);

            // Verify that a hard link has been created
            BasicFileAttributes fileAttributes =
                Files.readAttributes(objPath, BasicFileAttributes.class);
            BasicFileAttributes originalFileAttributes =
                Files.readAttributes(testDataFile, BasicFileAttributes.class);
            assertEquals(fileAttributes.fileKey(), originalFileAttributes.fileKey());
        }
    }

    /**
     * Check that storeHardLink does not throw exception when a hard link already exists
     */
    @Test
    public void storeHardLink_alreadyExists() throws Exception {
        for (String pid : testData.pidList) {
            String sha256 = testData.pidData.get(pid).get("sha256");
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            assertTrue(Files.exists(testDataFile));

            fileHashStoreLinks.storeHardLink(testDataFile, pid, sha256, "SHA-256");
            fileHashStoreLinks.storeHardLink(testDataFile, pid + ".test.pid", sha256, "SHA-256");
        }
    }

    /**
     * Check that storeHardLink throws nonMatchingChecksumException when values do not match
     */
    @Test
    public void storeHardLink_nonMatchingChecksum() {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            assertTrue(Files.exists(testDataFile));

            assertThrows(NonMatchingChecksumException.class,
                         () -> fileHashStoreLinks.storeHardLink(testDataFile, pid, "badchecksum",
                                                                "SHA-256"));

        }
    }

    /**
     * Confirm that generateChecksums calculates checksums as expected
     */
    @Test
    public void testGenerateChecksums() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStoreLinks.generateChecksums(dataStream, "MD2");

                String md2 = testData.pidData.get(pid).get("md2");
                String md5 = testData.pidData.get(pid).get("md5");
                String sha1 = testData.pidData.get(pid).get("sha1");
                String sha256 = testData.pidData.get(pid).get("sha256");
                String sha384 = testData.pidData.get(pid).get("sha384");
                String sha512 = testData.pidData.get(pid).get("sha512");
                assertEquals(md2, hexDigests.get("MD2"));
                assertEquals(md5, hexDigests.get("MD5"));
                assertEquals(sha1, hexDigests.get("SHA-1"));
                assertEquals(sha256, hexDigests.get("SHA-256"));
                assertEquals(sha384, hexDigests.get("SHA-384"));
                assertEquals(sha512, hexDigests.get("SHA-512"));

                assertEquals(hexDigests.size(), 6);
            }
        }
    }

    /**
     * Confirm that generateChecksums returns the default amount of checksums
     */
    @Test
    public void testGenerateChecksums_defaultChecksumsFound() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            // Get test file
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                Map<String, String> hexDigests =
                    fileHashStoreLinks.generateChecksums(dataStream, null);
                assertEquals(hexDigests.size(), 5);
            }
        }
    }
}

package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.filehashstore.FileHashStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import org.dataone.hashstore.testdata.TestDataHarness;

public class FileHashStoreLinksTest {

    private static Path rootDirectory;
    private static Path objStringFull;
    private static Path objTmpStringFull;
    private static Path metadataStringFull;
    private static Path metadataTmpStringFull;
    private static final TestDataHarness testData = new TestDataHarness();
    private static FileHashStoreLinks fileHashStoreLinks;

    /**
     * Initialize FileHashStore
     */
    @BeforeEach
    public void initializeFileHashStore() {
        Path root = tempFolder;
        rootDirectory = root.resolve("hashstore");
        objStringFull = rootDirectory.resolve("objects");
        objTmpStringFull = rootDirectory.resolve("objects/tmp");
        metadataStringFull = rootDirectory.resolve("metadata");
        metadataTmpStringFull = rootDirectory.resolve("metadata/tmp");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
        );

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
        Path refsPath = rootDirectory.resolve("refs");
        assertTrue(Files.isDirectory(refsPath));
        Path refsTmpPath = rootDirectory.resolve("refs/tmp");
        assertTrue(Files.isDirectory(refsTmpPath));
        Path refsPidPath = rootDirectory.resolve("refs/pids");
        assertTrue(Files.isDirectory(refsPidPath));
        Path refsCidPath = rootDirectory.resolve("refs/cids");
        assertTrue(Files.isDirectory(refsCidPath));
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
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
        );

        new FileHashStore(storeProperties);
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

            InputStream dataStream = Files.newInputStream(testDataFile);
            Map<String, String> hexDigests =
                fileHashStoreLinks.generateChecksums(dataStream, null, null);
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
}

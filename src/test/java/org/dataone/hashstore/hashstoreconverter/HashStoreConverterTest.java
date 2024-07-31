package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.HashStoreRefsAlreadyExistException;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for HashStoreConverter
 */
public class HashStoreConverterTest {
    private static Path rootDirectory;
    private static final TestDataHarness testData = new TestDataHarness();
    private HashStoreConverter hashstoreConverter;

    /**
     * Initialize FileHashStore
     */
    @BeforeEach
    public void initializeFileHashStore() {
        Path root = tempFolder;
        rootDirectory = root.resolve("hashstore");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
        );

        try {
            hashstoreConverter = new HashStoreConverter(storeProperties);

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
     * Check HashStoreConverter initializes with existing HashStore directory, does not throw
     * exception
     */
    @Test
    public void hashStoreConverter_hashStoreExists() {
        Path root = tempFolder;
        rootDirectory = root.resolve("hashstore");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
        );

        try {
            new HashStoreConverter(storeProperties);

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException encountered: " + e.getMessage());

        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

        }
    }

    /**
     * Check that convert creates hard link, stores sysmeta and returns the correct ObjectMetadata
     */
    @Test
    public void convert() throws Exception {
        for (String pid : testData.pidList) {
            // Path to test harness data file
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            // Path to metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            InputStream sysmetaStream = Files.newInputStream(testMetaDataFile);

            ObjectMetadata objInfo =
                hashstoreConverter.convert(testDataFile, pid, sysmetaStream);
            sysmetaStream.close();

            // Check checksums
            Map<String, String> hexDigests = objInfo.getHexDigests();
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
            assertEquals(sha256, objInfo.getCid());
            assertEquals(pid, objInfo.getPid());

            // Metadata is stored directly through 'FileHashStore'
            // Creation of hard links is confirmed via 'FileHashStoreLinks'
        }
    }

    /**
     * Check that convert throws 'HashStoreRefsAlreadyExistException' when called to store a
     * data object with a pid that has already been accounted for
     */
    @Test
    public void convert_duplicatePid() throws Exception {
        for (String pid : testData.pidList) {
            // Path to test harness data file
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            // Path to metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            InputStream sysmetaStream = Files.newInputStream(testMetaDataFile);
            hashstoreConverter.convert(testDataFile, pid, sysmetaStream);

            InputStream sysmetaStreamTwo = Files.newInputStream(testMetaDataFile);
            assertThrows(
                HashStoreRefsAlreadyExistException.class,
                () -> hashstoreConverter.convert(testDataFile, pid, sysmetaStreamTwo));
        }
    }

    /**
     * Confirm that convert still executes when filePath is null and stores the sysmeta
     */
    @Test
    public void convert_nullFilePath() throws Exception {
        for (String pid : testData.pidList) {
            // Path to test harness data file
            String pidFormatted = pid.replace("/", "_");
            // Path to metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            InputStream sysmetaStream = Files.newInputStream(testMetaDataFile);

            ObjectMetadata objInfo =
                hashstoreConverter.convert(null, pid, sysmetaStream);
            sysmetaStream.close();

            assertNull(objInfo);
        }
    }

    /**
     * Check that convert throws exception when sysmeta stream is null
     */
    @Test
    public void convert_nullSysmetaStream() {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            InputStream sysmetaStream = null;

            assertThrows(
                IllegalArgumentException.class, () -> hashstoreConverter.convert(testDataFile, pid, sysmetaStream)
            );
        }
    }
}

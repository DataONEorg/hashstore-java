package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test class for HashStoreConverter
 */
public class HashStoreConverterTest {
    private static Path rootDirectory;
    private static Path objStringFull;
    private static Path objTmpStringFull;
    private static Path metadataStringFull;
    private static Path metadataTmpStringFull;
    private static final TestDataHarness testData = new TestDataHarness();
    private HashStoreConverter hashstoreConverter;

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
     * Check object store and tmp directories exist after initialization
     */
    @Test
    public void initObjDirectories() {
        Path checkObjectStorePath = objStringFull;
        assertTrue(Files.isDirectory(checkObjectStorePath));
        Path checkTmpPath = objTmpStringFull;
        assertTrue(Files.isDirectory(checkTmpPath));
    }

    /**
     * Check HashStoreConverter initializes with existing HashStore directory, does not throw
     * exception
     */
    @Test
    public void hashStoreConverter_hashStoreExists() {
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
            new HashStoreConverter(storeProperties);

        } catch (IOException e) {
            e.printStackTrace();
            fail("IOException encountered: " + e.getMessage());

        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

        }
    }

}

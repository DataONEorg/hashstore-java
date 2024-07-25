package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class FileHashStoreLinksInitTest {

    private static Path rootDirectory;
    private static Path objStringFull;
    private static Path objTmpStringFull;
    private static Path metadataStringFull;
    private static Path metadataTmpStringFull;
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
}

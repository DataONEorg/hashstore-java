package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for FileHashStore references related methods
 */
public class FileHashStoreReferencesTest {
    private FileHashStore fileHashStore;
    private Properties fhsProperties;
    private Path rootDirectory;

    /**
     * Initialize FileHashStore before each test to creates tmp directories
     */
    @BeforeEach
    public void initializeFileHashStore() {
        rootDirectory = tempFolder.resolve("metacat");

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

        } catch (IOException ioe) {
            fail("IOException encountered: " + ioe.getMessage());

        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException encountered: " + nsae.getMessage());

        }
    }

    /**
     * Temporary folder for tests to run in
     */
    @TempDir
    public Path tempFolder;

    // TODO: Add tests for tag object
    // TODO: Add tests to check exception thrown when pid refs file already exists

    /**
     * Check that the cid supplied is written into the file given
     */
    @Test
    public void writePidRefsFile_Content() throws Exception {
        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        String cidToWrite = "test_cid_123";
        fileHashStore.writePidRefsFile(refsTmpFile, cidToWrite);

        String cidRead = new String(Files.readAllBytes(refsTmpFile.toPath()));
        assertEquals(cidRead, cidToWrite);

    }
}

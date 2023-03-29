package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import org.dataone.hashstore.hashfs.HashUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for HashStore utility methods
 */
public class HashUtilTest {

    /*
     * Non-test method using HashUtil class to generate a temp file
     */
    public File generateTemporaryFile() {
        String prefix = "testfile";
        File directory = tempFolder.getRoot();
        File newFile = null;
        try {
            HashUtil hsil = new HashUtil();
            newFile = hsil.generateTmpFile(prefix, directory);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
        return newFile;
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Confirm that a temporary file has been generated.
     */
    @Test
    public void testCreateTemporaryFile() {
        File newTmpFile = generateTemporaryFile();
        assertTrue(newTmpFile.exists());
    }

    /**
     * Check that the temporary file is not empty and that a list of
     * checksums are generated.
     */
    @Test
    public void testWriteToTempFileAndGenerateChecksums() {
        File newTmpFile = generateTemporaryFile();

        // Get test file
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", "jtao.1700.1");
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        try {
            InputStream dataStream = new FileInputStream(testDataFile);
            HashUtil hsil = new HashUtil();
            Map<String, String> hexDigests = hsil.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream, null);

            // Validate checksum values
            String md5 = "f4ea2d07db950873462a064937197b0f";
            String sha1 = "3d25436c4490b08a2646e283dada5c60e5c0539d";
            String sha256 = "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
            String sha384 = "a204678330fcdc04980c9327d4e5daf01ab7541e8a351d49a7e9c5005439dce749ada39c4c35f573dd7d307cca11bea8";
            String sha512 = "bf9e7f4d4e66bd082817d87659d1d57c2220c376cd032ed97cadd481cf40d78dd479cbed14d34d98bae8cebc603b40c633d088751f07155a94468aa59e2ad109";
            assertEquals(md5, hexDigests.get("MD-5"));
            assertEquals(sha1, hexDigests.get("SHA-1"));
            assertEquals(sha256, hexDigests.get("SHA-256"));
            assertEquals(sha384, hexDigests.get("SHA-384"));
            assertEquals(sha512, hexDigests.get("SHA-512"));

            long testDataFileSize = Files.size(testDataFile.toPath());
            Path tmpFilePath = newTmpFile.toPath();
            long tmpFileSize = Files.size(tmpFilePath);
            assertEquals(testDataFileSize, tmpFileSize);

        } catch (NoSuchAlgorithmException e) {
            fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }

    /**
     * Confirm that a digest is sharded appropriately
     */
    @Test
    public void testShardHexDigest() {
        HashUtil hsil = new HashUtil();
        String shardedPath = hsil.shard(3, 2, "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        String shardedPathExpected = "94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
    }
}
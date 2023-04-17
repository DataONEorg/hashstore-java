package org.dataone.hashstore.hashfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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

import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for HashStore utility methods
 */
public class HashUtilTest {
    public TestDataHarness testData = new TestDataHarness();

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
     * Confirm that a digest is sharded appropriately
     */
    @Test
    public void testShardHexDigest() {
        HashUtil hsil = new HashUtil();
        String shardedPath = hsil.shard(3, 2, "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a");
        String shardedPathExpected = "/94/f9/b6/c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";
        assertEquals(shardedPath, shardedPathExpected);
    }

    /**
     * Check algorithm support
     */
    @Test
    public void testValidateAlgorithm() {
        HashUtil hsil = new HashUtil();

        String md2 = "MD2";
        boolean supported = hsil.validateAlgorithm(md2);
        assertTrue(supported);

        String sm3 = "SM3";
        boolean not_supported = hsil.validateAlgorithm(sm3);
        assertFalse(not_supported);

        // Must match string to reduce complexity, no string formatting
        String md2_lowercase = "md2";
        boolean lowercase_not_supported = hsil.validateAlgorithm(md2_lowercase);
        assertFalse(lowercase_not_supported);
    }

    @Test
    public void testGetHexDigest() {
        HashUtil hsil = new HashUtil();
        try {
            for (String pid : this.testData.pidList) {
                String abIdDigest = hsil.getHexDigest(pid, "SHA-256");
                String abIdTestData = this.testData.pidData.get(pid).get("s_cid");
                assertEquals(abIdDigest, abIdTestData);
            }
        } catch (NoSuchAlgorithmException nsae) {
            fail("NoSuchAlgorithmException: " + nsae.getMessage());
        }
    }

    /**
     * Check that the temporary file is not empty and that a list of
     * checksums are generated.
     */
    @Test
    public void testWriteToTempFileAndGenerateChecksums() {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            try {
                // Extra algo to calculate - MD2
                String addAlgo = "MD2";

                InputStream dataStream = new FileInputStream(testDataFile);
                HashUtil hsil = new HashUtil();
                Map<String, String> hexDigests = hsil.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream,
                        addAlgo);

                // Validate checksum values
                String md5 = this.testData.pidData.get(pid).get("md5");
                String sha1 = this.testData.pidData.get(pid).get("sha1");
                String sha256 = this.testData.pidData.get(pid).get("sha256");
                String sha384 = this.testData.pidData.get(pid).get("sha384");
                String sha512 = this.testData.pidData.get(pid).get("sha512");
                assertEquals(md5, hexDigests.get("MD5"));
                assertEquals(sha1, hexDigests.get("SHA-1"));
                assertEquals(sha256, hexDigests.get("SHA-256"));
                assertEquals(sha384, hexDigests.get("SHA-384"));
                assertEquals(sha512, hexDigests.get("SHA-512"));

                // Validate additional algorithm
                String md2 = this.testData.pidData.get(pid).get("md2");
                assertEquals(md2, hexDigests.get("MD2"));

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
    }

    /**
     * Check that exception is thrown when unsupported algorithm supplied
     */
    @Test(expected = IllegalArgumentException.class)
    public void testWriteToTempFileAndGenerateChecksumsInvalidAlgo() {
        for (String pid : this.testData.pidList) {
            File newTmpFile = generateTemporaryFile();
            String pidFormatted = pid.replace("/", "_");

            // Get test file
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            try {
                // Extra algo to calculate - MD2
                String addAlgo = "SM2";

                InputStream dataStream = new FileInputStream(testDataFile);
                HashUtil hsil = new HashUtil();
                Map<String, String> hexDigests = hsil.writeToTmpFileAndGenerateChecksums(newTmpFile, dataStream,
                        addAlgo);

            } catch (NoSuchAlgorithmException e) {
                fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
            } catch (IOException e) {
                fail("IOException: " + e.getMessage());
            }
        }
    }

    /**
     * Confirm that object has moved
     */
    @Test
    public void testMove() {
        HashUtil hsil = new HashUtil();

        File newTmpFile = generateTemporaryFile();
        String targetString = tempFolder.getRoot().toString() + "/testmove/test_tmp_object.tmp";
        File targetFile = new File(targetString);

        try {
            hsil.move(newTmpFile, targetFile);
            assertTrue(targetFile.exists());
        } catch (IOException e) {
            fail("IOException: " + e.getMessage());
        }
    }
}
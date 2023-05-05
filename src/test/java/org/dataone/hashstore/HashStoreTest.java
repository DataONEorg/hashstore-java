package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dataone.hashstore.hashfs.HashAddress;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for HashStore-java
 */
public class HashStoreTest {
    public HashStore hashStore;
    public Path rootPathFull;
    public TestDataHarness testData = new TestDataHarness();

    /**
     * Generates a hierarchical path by dividing a given digest into tokens
     * of fixed width, and concatenating them with '/' as the delimiter.
     *
     * @param depth  integer to represent number of directories
     * @param width  width of each directory
     * @param digest value to shard
     * @return String
     */
    public String shard(int depth, int width, String digest) {
        List<String> tokens = new ArrayList<>();
        int digestLength = digest.length();
        for (int i = 0; i < depth; i++) {
            int start = i * width;
            int end = Math.min((i + 1) * width, digestLength);
            tokens.add(digest.substring(start, end));
        }
        if (depth * width < digestLength) {
            tokens.add(digest.substring(depth * width));
        }
        List<String> stringArray = new ArrayList<>();
        for (String str : tokens) {
            if (!str.trim().isEmpty()) {
                stringArray.add(str);
            }
        }
        String stringShard = String.join("/", stringArray);
        return stringShard;
    }

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void initializeHashStore() {
        Path rootDirectory = tempFolder.getRoot().toPath();
        String rootString = rootDirectory.toString();
        String rootStringFull = rootString + "/metacat";
        this.rootPathFull = Paths.get(rootStringFull);
        try {
            this.hashStore = new HashStore(rootPathFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Check that object storeDirectory is created
     */
    @Test
    public void hashStoreConstructor_storeDirectory() {
        Path checkStoreObjPath = rootPathFull.resolve("objects");
        assertTrue(Files.exists(checkStoreObjPath));
    }

    /**
     * Check that object tmp storeDirectory is created
     */
    @Test
    public void hashStoreConstructor_storeTmpDirectory() {
        Path checkStoreObjTmpPath = rootPathFull.resolve("objects/tmp");
        assertTrue(Files.exists(checkStoreObjTmpPath));
    }

    /**
     * Check that store object returns the correct HashAddress object id
     */
    @Test
    public void storeObject() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, objInfo.getId());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object rel path
     */
    @Test
    public void storeObject_relPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check relative path
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            String objRelPath = this.shard(3, 2, objAuthorityId);
            assertEquals(objRelPath, objInfo.getRelPath());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object abs path
     */
    @Test
    public void storeObject_absPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check absolute path
            File objAbsPath = new File(objInfo.getAbsPath());
            assertTrue(objAbsPath.exists());
        }
    }

    /**
     * Check that store object moves file successfully (isDuplicate == false)
     */
    @Test
    public void storeObject_isDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check duplicate status
            assertFalse(objInfo.getIsDuplicate());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object hex digests
     */
    @Test
    public void storeObject_hexDigests() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            Map<String, String> hexDigests = objInfo.getHexDigests();

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
        }
    }

    /**
     * Check that store object throws exception when object is null
     */
    @Test(expected = NullPointerException.class)
    public void storeObject_null() throws Exception {
        String pid = "j.tao.1700.1";
        hashStore.storeObject(null, pid, null, null, null);
    }

    /**
     * Check that store object throws exception when pid is null
     */
    @Test(expected = IllegalArgumentException.class)
    public void storeObject_nullPid() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            hashStore.storeObject(dataStream, null, null, null, null);

        }
    }

    /**
     * Check that store object throws exception when pid is empty
     */
    @Test(expected = IllegalArgumentException.class)
    public void storeObject_emptyPid() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            hashStore.storeObject(dataStream, "", null, null, null);

        }
    }

    /**
     * Verify that storeObject stores an object with a good checksum value
     */
    @Test
    public void storeObject_validateChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        String checksumCorrect = "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";

        InputStream dataStream = Files.newInputStream(testDataFile);
        HashAddress address = hashStore.storeObject(dataStream, pid, null, checksumCorrect, "SHA-256");

        File objAbsPath = new File(address.getAbsPath());
        assertTrue(objAbsPath.exists());
    }

    /**
     * Verify that storeObject throws an exception when expected to validate object
     * but checksum is not available/part of the hex digest map
     */
    @Test(expected = IllegalArgumentException.class)
    public void storeObject_missingChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        HashAddress address = hashStore.storeObject(dataStream, pid, null, checksumCorrect, "MD2");

        File objAbsPath = new File(address.getAbsPath());
        assertTrue(objAbsPath.exists());
    }

    /**
     * Verify that storeObject generates an additional checksum
     */
    @Test
    public void storeObject_correctChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, "MD2", null, null);

        String md2 = this.testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test(expected = IllegalArgumentException.class)
    public void storeObject_incorrectChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        String checksumIncorrect = "aaf9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, null, checksumIncorrect, "SHA-256");
    }

    /**
     * Verify exception thrown when checksum is empty and algorithm supported
     */
    @Test(expected = IllegalArgumentException.class)
    public void storeObject_emptyChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        String checksumEmpty = "";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, null, checksumEmpty, "MD2");
    }

    /**
     * Verify exception thrown when checksum is null and algorithm supported
     */
    @Test(expected = NullPointerException.class)
    public void storeObject_nullChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, null, null, "SHA-512/224");
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test(expected = IllegalArgumentException.class)
    public void put_invalidAlgorithm() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, "SM2", null, null);
    }

    /**
     * Check that store object throws FileAlreadyExists error when storing duplicate
     * object
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void storeObject_duplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            hashStore.storeObject(dataStream, pid, null, null, null);

            InputStream dataStreamDup = Files.newInputStream(testDataFile);
            hashStore.storeObject(dataStreamDup, pid, null, null, null);
        }
    }

    /**
     * Tests that the `storeObject` method can store an object successfully with
     * multiple threads (3). This test uses three futures (threads) that run
     * concurrently, all except one of which will encounter an `ExecutionException`.
     * The thread that does not encounter an exception will store the given
     * object, and verifies that the object is stored successfully.
     * 
     * The test expects exceptions to be encountered, which can be either a
     * `RunTimeException` or a `FileAlreadyExistsException`. This is because the
     * rapid execution of threads can result in bypassing the object lock and
     * the failure to throw a RunTimeException. However, since the file should
     * already have been written to disk, a`FileAlreadyExistsException` will be
     * thrown, ensuring that an object is never stored twice.
     */
    @Test
    public void storeObject_objectLockedIds() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        // Create a thread pool with 3 threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Submit 3 threads, each calling storeObject
        Future<?> future1 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);
                if (objInfo != null) {
                    String absPath = objInfo.getAbsPath();
                    File permAddress = new File(absPath);
                    assertTrue(permAddress.exists());
                }
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeException || e instanceof FileAlreadyExistsException);
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);
                if (objInfo != null) {
                    String absPath = objInfo.getAbsPath();
                    File permAddress = new File(absPath);
                    assertTrue(permAddress.exists());
                }
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeException || e instanceof FileAlreadyExistsException);
            }
        });
        Future<?> future3 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);
                if (objInfo != null) {
                    String absPath = objInfo.getAbsPath();
                    File permAddress = new File(absPath);
                    assertTrue(permAddress.exists());
                }
            } catch (Exception e) {
                assertTrue(e instanceof RuntimeException || e instanceof FileAlreadyExistsException);
            }
        });

        // Wait for all tasks to complete and check results
        // .get() on the future ensures that all tasks complete before the test ends
        future1.get();
        future2.get();
        future3.get();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

}

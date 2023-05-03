package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
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
    public void storeObject_isNotDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            Path testDataFile = new File(testdataAbsolutePath).toPath();

            InputStream dataStream = Files.newInputStream(testDataFile);
            HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);

            // Check duplicate status
            assertTrue(objInfo.getIsNotDuplicate());
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
     * Verify that additional checksum is generated/validated
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
        hashStore.storeObject(dataStream, pid, "MD2", checksumCorrect, "MD2");

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

        String checksumIncorrect = "1c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        hashStore.storeObject(dataStream, pid, "MD2", checksumIncorrect, "MD2");
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
        hashStore.storeObject(dataStream, pid, "MD2", checksumEmpty, "MD2");
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
     * Check store object pid lock for duplicate object file exists.
     * 
     * Two futures (threads) will run concurrently, one of which will encounter an
     * ExecutionException, and the other will store the given object. The future
     * that stores the object successfully (obj != null) is checked to ensure
     * that the file has been written and moved as intended.
     */
    @Test
    public void storeObject_objectLockedIdsPidFileMoved() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        // Create a thread pool with 2 threads
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Submit 2 threads, each calling storeObject
        executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                HashAddress objInfo = hashStore.storeObject(dataStream, pid, null, null, null);
                if (objInfo != null) {
                    String absPath = objInfo.getAbsPath();
                    File permAddress = new File(absPath);
                    assertTrue(permAddress.exists());
                }
            } catch (Exception e) {
                fail("future - Unexpected Exception: " + e.getMessage());
            }
        });
        executorService.submit(() -> {
            try {
                InputStream dataStreamDup = Files.newInputStream(testDataFile);
                HashAddress objInfoDup = hashStore.storeObject(dataStreamDup, pid, null, null, null);
                if (objInfoDup != null) {
                    String absPath = objInfoDup.getAbsPath();
                    File permAddress = new File(absPath);
                    assertTrue(permAddress.exists());
                }
            } catch (Exception e) {
                fail("future_dup - Unexpected Exception: " + e.getMessage());
            }
        });

        // Wait for all tasks to complete
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Check store object pid lock for duplicate object exception
     * 
     * Two futures (threads) will run concurrently, one of which will encounter an
     * ExecutionException. The future that yields the exception is then checked to
     * confirm that a FileAlreadyExistsException exception is thrown.
     */
    @Test
    public void storeObject_objectLockedIdsFileExistsException() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        Path testDataFile = new File(testdataAbsolutePath).toPath();

        // Create a thread pool with 2 threads
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Submit 2 threads, each calling storeObject
        executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                hashStore.storeObject(dataStream, pid, null, null, null);
            } catch (Exception e) {
                assertTrue(e instanceof FileAlreadyExistsException);
            }
        });
        executorService.submit(() -> {
            try {
                InputStream dataStreamDup = Files.newInputStream(testDataFile);
                hashStore.storeObject(dataStreamDup, pid, null, null, null);
            } catch (Exception e) {
                assertTrue(e instanceof FileAlreadyExistsException);
            }
        });

        // Wait for all tasks to complete
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

}

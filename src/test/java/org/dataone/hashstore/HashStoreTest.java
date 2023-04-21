package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.dataone.hashstore.hashfs.HashAddress;
import org.dataone.hashstore.hashfs.HashUtil;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for HashStore-java
 */
public class HashStoreTest {
    public HashStore hsj;
    public Path rootDirectory;
    public String rootString;
    public String rootStringFull;

    public TestDataHarness testData = new TestDataHarness();
    public HashUtil hsil = new HashUtil();

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Before
    public void initializeHashStore() {
        this.rootDirectory = tempFolder.getRoot().toPath();
        this.rootString = rootDirectory.toString();
        this.rootStringFull = rootString + "/metacat";
        try {
            this.hsj = new HashStore(rootStringFull);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
    }

    /**
     * Check that object storeDirectory is created
     */
    @Test
    public void testHashStoreConstructorStoreDirectory() {
        Path checkStoreObjPath = Paths.get(this.rootStringFull).resolve("objects");
        assertTrue(Files.exists(checkStoreObjPath));
    }

    /**
     * Check that object tmp storeDirectory is created
     */
    @Test
    public void testHashStoreConstructorStoreTmpDirectory() {
        Path checkStoreObjTmpPath = Paths.get(this.rootStringFull).resolve("objects/tmp");
        assertTrue(Files.exists(checkStoreObjTmpPath));
    }

    /**
     * Check that store object returns the correct HashAddress object id
     */
    @Test
    public void testStoreObject() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

            // Check id (sha-256 hex digest of the ab_id, aka s_cid)
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            assertEquals(objAuthorityId, objInfo.getId());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object rel path
     */
    @Test
    public void testStoreObjectRelPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

            // Check relative path
            String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
            String objRelPath = this.hsil.shard(3, 2, objAuthorityId);
            assertEquals(objRelPath, objInfo.getRelPath());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object abs path
     */
    @Test
    public void testStoreObjectAbsPath() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

            // Check absolute path
            File objAbsPath = new File(objInfo.getAbsPath());
            assertTrue(objAbsPath.exists());
        }
    }

    /**
     * Check that store object moves file successfully (isDuplicate == false)
     */
    @Test
    public void testStoreObjectMoveIsDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

            // Check duplicate status
            assertFalse(objInfo.getIsDuplicate());
        }
    }

    /**
     * Check that store object returns the correct HashAddress object hex digests
     */
    @Test
    public void testStoreObjectHexDigests() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

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
     * Check that store object throws FileAlreadyExists error when storing duplicate
     * object
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void testStoreObjectDuplicate() throws Exception {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            InputStream dataStream = new FileInputStream(testDataFile);
            HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);

            InputStream dataStreamDup = new FileInputStream(testDataFile);
            HashAddress objInfoDup = hsj.storeObject(dataStreamDup, pid, null, null, null);
        }
    }

    /**
     * Check store object pid lock for duplicate object file exists.
     * 
     * Two threads will run concurrently, one of which will encounter an
     * ExecutionException (which is what is thrown when using Executors)
     * and the other will store the given object. The HashAddress object that is not
     * null, checks that the file has been written and moved successfully.
     */
    @Test
    public void testStoreObjectObjectLockedIdsPidFileExists() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        // Create a thread pool with 2 threads
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Submit 2 threads, each calling storeObject
        executorService.submit(() -> {
            try {
                InputStream dataStream = new FileInputStream(testDataFile);
                HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);
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
                InputStream dataStreamDup = new FileInputStream(testDataFile);
                HashAddress objInfoDup = hsj.storeObject(dataStreamDup, pid, null, null, null);
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
     * ExecutionException. The future that yields the exception is then parsed to
     * confirm that a FileAlreadyExistsException was encountered.
     */
    @Test(expected = FileAlreadyExistsException.class)
    public void testStoreObjectObjectLockedIds() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore", "testdata", pid);
        String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
        File testDataFile = new File(testdataAbsolutePath);

        // Create a thread pool with 2 threads
        ExecutorService executorService = Executors.newFixedThreadPool(2);

        // Submit 2 threads, each calling storeObject
        Future<?> future = executorService.submit(() -> {
            try {
                InputStream dataStream = new FileInputStream(testDataFile);
                HashAddress objInfo = hsj.storeObject(dataStream, pid, null, null, null);
            } catch (FileAlreadyExistsException e) {
                fail("future - FileAlreadyExistsException Exception: " + e.getMessage());
            } catch (Exception e) {
                fail("future - Unexpected Exception: " + e.getMessage());
            }
        });
        Future<?> future_dup = executorService.submit(() -> {
            try {
                InputStream dataStreamDup = new FileInputStream(testDataFile);
                HashAddress objInfoDup = hsj.storeObject(dataStreamDup, pid, null, null, null);
            } catch (FileAlreadyExistsException e) {
                fail("future_dup - FileAlreadyExistsException Exception: " + e.getMessage());
            } catch (Exception e) {
                fail("future_dup - Unexpected Exception: " + e.getMessage());
            }
        });

        // Wait for all tasks to complete
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // Get exception message and confirm FileAlreadyExistsException
        try {
            future.get();
            future_dup.get();
        } catch (ExecutionException e) {
            String cause = e.getMessage();
            if (cause.contains("FileAlreadyExistsException")) {
                throw new FileAlreadyExistsException(cause);
            }
        }
    }

}

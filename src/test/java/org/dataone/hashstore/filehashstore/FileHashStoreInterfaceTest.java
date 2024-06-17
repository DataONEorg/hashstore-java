package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.HashStoreRunnable;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.OrphanPidRefsFileException;
import org.dataone.hashstore.exceptions.OrphanRefsFilesException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.filehashstore.FileHashStore.HashStoreIdTypes;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


/**
 * Test class for FileHashStore HashStore Interface methods.
 * 
 * Note: `tagObject` & `verifyObject` tests can be found in the `FileHashStoreReferences` class
 */
public class FileHashStoreInterfaceTest {
    private FileHashStore fileHashStore;
    private Properties fhsProperties;
    private Path rootDirectory;
    private static final TestDataHarness testData = new TestDataHarness();
    private String fhsDeleteTypePid;
    private String fhsDeleteTypeCid;

    /**
     * Initialize FileHashStore before each test to creates tmp directories
     */
    @BeforeEach
    public void initializeFileHashStore() {
        rootDirectory = tempFolder.resolve("metacat");
        fhsDeleteTypePid = HashStoreIdTypes.pid.getName();
        fhsDeleteTypeCid = HashStoreIdTypes.cid.getName();

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
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

    /**
     * Check that store object returns the correct ObjectMetadata id
     */
    @Test
    public void storeObject() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            // Check id (content identifier based on the store algorithm)
            String objectCid = testData.pidData.get(pid).get("sha256");
            assertEquals(objectCid, objInfo.getCid());
            assertEquals(pid, objInfo.getPid());
        }
    }

    /**
     * Check that store object returns the correct ObjectMetadata size
     */
    @Test
    public void storeObject_objSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            // Check the object size
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object returns the correct ObjectMetadata hex digests
     */
    @Test
    public void storeObject_hexDigests() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            Map<String, String> hexDigests = objInfo.getHexDigests();

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

    /**
     * Check that store object throws exception when object is null
     */
    @Test
    public void storeObject_null() {
        assertThrows(IllegalArgumentException.class, () -> {
            String pid = "j.tao.1700.1";
            fileHashStore.storeObject(null, pid, null, null, null, -1);
        });
    }

    /**
     * Check that store object throws exception when pid is null
     */
    @Test
    public void storeObject_nullPid() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStream, null, null, null, null, -1);
                dataStream.close();
            });
        }
    }

    /**
     * Check that store object throws exception when pid is empty
     */
    @Test
    public void storeObject_emptyPid() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStream, "", null, null, null, -1);
                dataStream.close();
            });
        }
    }

    /**
     * Check that store object throws exception when object size is 0
     */
    @Test
    public void storeObject_zeroObjSize() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStream, pid, null, null, null, 0);
                dataStream.close();
            });
        }
    }

    /**
     * Verify that storeObject stores and validates a given checksum and its expected size
     * with overloaded method
     */
    @Test
    public void storeObject_overloadChecksumCsAlgoAndSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            String md2 = testData.pidData.get(pid).get("md2");
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, md2, "MD2", objectSize
            );
            dataStream.close();

            Map<String, String> hexDigests = objInfo.getHexDigests();

            // Validate checksum values
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Verify that storeObject stores and validates a given checksum with overloaded method
     */
    @Test
    public void storeObject_overloadChecksumAndChecksumAlgo() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            String md2 = testData.pidData.get(pid).get("md2");

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, md2, "MD2");
            dataStream.close();

            Map<String, String> hexDigests = objInfo.getHexDigests();

            // Validate checksum values
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Check that store object returns the correct ObjectMetadata size with overloaded method
     */
    @Test
    public void storeObject_overloadObjSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, objectSize);
            dataStream.close();

            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object executes as expected with only an InputStream (does not create
     * any reference files)
     */
    @Test
    public void storeObject_overloadInputStreamOnly() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            Map<String, String> hexDigests = objInfo.getHexDigests();
            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");
            String cid = objInfo.getCid();

            assertEquals(hexDigests.get(defaultStoreAlgorithm), cid);

            assertThrows(FileNotFoundException.class, () -> {
                fileHashStore.findObject(pid);
            });

            Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            assertFalse(Files.exists(cidRefsFilePath));
        }
    }

    /**
     * Verify that storeObject generates an additional checksum with overloaded method
     */
    @Test
    public void storeObject_overloadAdditionalAlgo() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, "MD2");
            dataStream.close();

            Map<String, String> hexDigests = objInfo.getHexDigests();

            // Validate checksum values
            String md2 = testData.pidData.get(pid).get("md2");
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Verify that storeObject returns the expected checksum value
     */
    @Test
    public void storeObject_validateChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumCorrect = "94f9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.storeObject(dataStream, pid, null, checksumCorrect, "SHA-256", -1);
        dataStream.close();

        Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
        assertTrue(Files.exists(objCidAbsPath));
    }

    /**
     * Verify that storeObject generates an additional checksum
     */
    @Test
    public void storeObject_correctChecksumValue() throws Exception {
        // Get test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        String checksumCorrect = "9c25df1c8ba1d2e57bb3fd4785878b85";

        InputStream dataStream = Files.newInputStream(testDataFile);
        fileHashStore.storeObject(dataStream, pid, "MD2", null, null, -1);
        dataStream.close();

        String md2 = testData.pidData.get(pid).get("md2");
        assertEquals(checksumCorrect, md2);
    }

    /**
     * Verify exception thrown when checksum provided does not match
     */
    @Test
    public void storeObject_incorrectChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            String checksumIncorrect =
                "aaf9b6c88f1f458e410c30c351c6384ea42ac1b5ee1f8430d3e365e43b78a38a";

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, checksumIncorrect, "SHA-256", -1);
            dataStream.close();
        });
    }

    /**
     * Verify exception thrown when checksum is empty and algorithm supported
     */
    @Test
    public void storeObject_emptyChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            String checksumEmpty = "";

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, checksumEmpty, "MD2", -1);
            dataStream.close();
        });
    }

    /**
     * Verify exception thrown when checksum is null and algorithm supported
     */
    @Test
    public void storeObject_nullChecksumValue() {
        assertThrows(IllegalArgumentException.class, () -> {
            // Get single test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, "SHA-512/224", -1);
            dataStream.close();
        });
    }

    /**
     * Check that store object throws exception when incorrect file size provided
     */
    @Test
    public void storeObject_objSizeCorrect() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, objectSize
            );
            dataStream.close();

            // Check id (sha-256 hex digest of the ab_id (pid))
            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object throws exception when incorrect file size provided
     */
    @Test
    public void storeObject_objSizeIncorrect() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 1000
                );
                dataStream.close();

                // Check id (sha-256 hex digest of the ab_id (pid))
                long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
                assertEquals(objectSize, objInfo.getSize());
            });
        }
    }

    /**
     * Verify exception thrown when unsupported additional algorithm provided
     */
    @Test
    public void storeObject_invalidAlgorithm() {
        assertThrows(NoSuchAlgorithmException.class, () -> {
            // Get single test file to "upload"
            String pid = "jtao.1700.1";
            Path testDataFile = testData.getTestFile(pid);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, "SM2", null, null, -1);
            dataStream.close();
        });
    }

    /**
     * Check that store object tags cid refs file as expected when called
     * to store a duplicate object (two pids that reference the same cid)
     */
    @Test
    public void storeObject_duplicate() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            String pidTwo = pid + ".test";
            InputStream dataStreamDup = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStreamDup, pidTwo, null, null, null, -1
            );
            dataStreamDup.close();

            String cid = objInfo.getCid();
            Path absCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            assertTrue(fileHashStore.isStringInRefsFile(pid, absCidRefsPath));
            assertTrue(fileHashStore.isStringInRefsFile(pidTwo, absCidRefsPath));
        }
    }

    /**
     * Test that storeObject successfully stores a 1GB file
     * 
     * Note 1: a 4GB successfully stored in approximately 1m30s
     * Note 2: Successfully stores 250GB file confirmed from knbvm
     */
    @Test
    public void storeObject_largeSparseFile() throws Exception {
        long fileSize = 1L * 1024L * 1024L * 1024L; // 1GB
        // Get tmp directory to initially store test file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        Path testFilePath = storePath.resolve("random_file.bin");

        // Generate a random file with the specified size
        try (FileOutputStream fileOutputStream = new FileOutputStream(testFilePath.toString())) {
            FileChannel fileChannel = fileOutputStream.getChannel();
            FileLock lock = fileChannel.lock();
            fileChannel.position(fileSize - 1);
            fileChannel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));
            lock.release();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }

        InputStream dataStream = Files.newInputStream(testFilePath);
        String pid = "dou.sparsefile.1";
        fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
        dataStream.close();

        Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
        assertTrue(Files.exists(objCidAbsPath));

    }

    /**
     * Tests that temporary objects that are being worked on while storeObject is in
     * progress and gets interrupted are deleted.
     */
    @Test
    public void storeObject_interruptProcess() throws Exception {
        long fileSize = 1L * 1024L * 1024L * 1024L; // 1GB
        // Get tmp directory to initially store test file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        Path testFilePath = storePath.resolve("random_file.bin");

        // Generate a random file with the specified size
        try (FileOutputStream fileOutputStream = new FileOutputStream(testFilePath.toString())) {
            FileChannel fileChannel = fileOutputStream.getChannel();
            FileLock lock = fileChannel.lock();
            fileChannel.position(fileSize - 1);
            fileChannel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));
            lock.release();
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw ioe;
        }

        Thread toInterrupt = new Thread(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testFilePath);
                String pid = "dou.sparsefile.1";
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
                dataStream.close();
            } catch (IOException | NoSuchAlgorithmException | InterruptedException ioe) {
                ioe.printStackTrace();
            }
        });

        toInterrupt.start();
        Thread.sleep(5000);
        toInterrupt.interrupt();
        toInterrupt.join();

        // Confirm there are no files in 'objects/tmp' directory
        File[] files = storePath.resolve("objects/tmp").toFile().listFiles();
        assert files != null;
        assertEquals(0, files.length);
    }

    /**
     * Tests that the `storeObject` method can store an object successfully with multiple threads
     * (5). This test uses five futures (threads) that run concurrently, all except one of which
     * will encounter a `RunTimeException`. The thread that does not encounter an exception will
     * store the given object, and verifies that the object is stored successfully.
     * 
     * The threads are expected to encounter a `RunTimeException` since the expected
     * object to store is already in progress (thrown by `syncPutObject` which coordinates
     * `store_object` requests with a pid). If both threads execute simultaneously and bypasses
     * the store object synchronization flow, we may also run into a `PidRefsFileExistsException`
     * - which prevents the cid from being tagged twice by the same pid.
     */
    @Test
    public void storeObject_objectLockedIds_FiveThreads() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        // Create a thread pool with 5 threads
        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // Submit 5 futures to the thread pool, each calling storeObject
        Future<?> future1 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, -1
                );
                dataStream.close();
                if (objInfo != null) {
                    String cid = objInfo.getCid();
                    Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
                    Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
                    Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
                    assertTrue(Files.exists(objCidAbsPath));
                    assertTrue(Files.exists(pidRefsPath));
                    assertTrue(Files.exists(cidRefsPath));
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException | e instanceof PidRefsFileExistsException);
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, -1
                );
                dataStream.close();
                if (objInfo != null) {
                    String cid = objInfo.getCid();
                    Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
                    Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
                    Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
                    assertTrue(Files.exists(objCidAbsPath));
                    assertTrue(Files.exists(pidRefsPath));
                    assertTrue(Files.exists(cidRefsPath));
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException | e instanceof PidRefsFileExistsException);
            }
        });
        Future<?> future3 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, -1
                );
                dataStream.close();
                if (objInfo != null) {
                    String cid = objInfo.getCid();
                    Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
                    Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
                    Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
                    assertTrue(Files.exists(objCidAbsPath));
                    assertTrue(Files.exists(pidRefsPath));
                    assertTrue(Files.exists(cidRefsPath));
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException | e instanceof PidRefsFileExistsException);
            }
        });
        Future<?> future4 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, -1
                );
                dataStream.close();
                if (objInfo != null) {
                    String cid = objInfo.getCid();
                    Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
                    Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
                    Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
                    assertTrue(Files.exists(objCidAbsPath));
                    assertTrue(Files.exists(pidRefsPath));
                    assertTrue(Files.exists(cidRefsPath));
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException | e instanceof PidRefsFileExistsException);
            }
        });
        Future<?> future5 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, -1
                );
                dataStream.close();
                if (objInfo != null) {
                    String cid = objInfo.getCid();
                    Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
                    Path pidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
                    Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
                    assertTrue(Files.exists(objCidAbsPath));
                    assertTrue(Files.exists(pidRefsPath));
                    assertTrue(Files.exists(cidRefsPath));
                }
            } catch (Exception e) {
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException | e instanceof PidRefsFileExistsException);
            }
        });

        // Wait for all tasks to complete and check results
        // .get() on the future ensures that all tasks complete before the test ends
        future1.get();
        future2.get();
        future3.get();
        future4.get();
        future5.get();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
    }

    /**
     * Test storeObject synchronization using a Runnable class
     */
    @Test
    public void storeObject_50Pids_1Obj_viaRunnable() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        List<String> pidModifiedList = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            pidModifiedList.add(pid + ".dou.test." + i);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        for (String pidAdjusted : pidModifiedList) {
            InputStream dataStream = Files.newInputStream(testDataFile);
            HashStoreRunnable
                request = new HashStoreRunnable(fileHashStore, 1, dataStream, pidAdjusted);
            executorService.execute(request);
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // Check cid refs file that every pid is found
        String cidSha256DigestFromTestData = testData.pidData.get(pid).get("sha256");
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cidSha256DigestFromTestData, "refs", "cid");
        Set<String> stringSet = new HashSet<>(pidModifiedList);
        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean allFoundPidsFound = true;
        for (String line : lines) {
            if (!stringSet.contains(line)) {
                allFoundPidsFound = false;
                break;
            }
        }
        assertTrue(allFoundPidsFound);

        // Confirm that 50 pid refs file exists
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefFiles = FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs"
                                                                                            + "/pids"));
        assertEquals(50, pidRefFiles.size());
    }

    /**
     * Test storeMetadata stores metadata as expected
     */
    @Test
    public void storeMetadata() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String testFormatId = "https://test.arcticdata.io/ns";
            String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, testFormatId);
            metadataStream.close();

            // Calculate absolute path
            Path metadataPidExpectedPath = fileHashStore.getExpectedPath(
                pid, "metadata", testFormatId
            );

            assertEquals(metadataPidExpectedPath.toString(), metadataPath);
            assertTrue(Files.exists(metadataPidExpectedPath));
        }
    }

    /**
     * Test storeMetadata with overload method (default namespace)
     */
    @Test
    public void storeMetadata_defaultFormatId_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String metadataPath = fileHashStore.storeMetadata(metadataStream, pid);
            metadataStream.close();

            // Calculate absolute path
            String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
            Path metadataPidExpectedPath = fileHashStore.getExpectedPath(
                pid, "metadata", storeMetadataNamespace
            );

            assertEquals(metadataPidExpectedPath.toString(), metadataPath);
            assertTrue(Files.exists(metadataPidExpectedPath));
        }
    }

    /**
     * Test storeMetadata creates appropriate directory for metadata documents with the given pid
     */
    @Test
    public void storeMetadata_pidHashIsDirectory() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid);
            metadataStream.close();

            String storeAlgo = fhsProperties.getProperty("storeAlgorithm");
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String metadataPidhash = FileHashStoreUtility.getPidHexDigest(pid, storeAlgo);
            String pidMetadataDirectory = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, metadataPidhash
            );
            Path expectedPidMetadataDirectory = rootDirectory.resolve("metadata").resolve(
                pidMetadataDirectory
            );

            assertTrue(Files.isDirectory(expectedPidMetadataDirectory));
        }
    }

    /**
     * Test storeMetadata stores different metadata for a given pid in its expected directory
     */
    @Test
    public void storeMetadata_multipleFormatIds() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String testFormatId = "https://test.arcticdata.io/ns";
            String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, testFormatId);
            metadataStream.close();

            InputStream metadataStreamDup = Files.newInputStream(testMetaDataFile);
            String metadataDefaultPath = fileHashStore.storeMetadata(metadataStreamDup, pid);
            metadataStreamDup.close();

            // Calculate absolute path
            Path metadataTestFormatIdExpectedPath = fileHashStore.getExpectedPath(
                pid, "metadata", testFormatId
            );
            String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
            Path metadataDefaultExpectedPath = fileHashStore.getExpectedPath(
                pid, "metadata", storeMetadataNamespace
            );

            assertEquals(metadataTestFormatIdExpectedPath.toString(), metadataPath);
            assertTrue(Files.exists(metadataTestFormatIdExpectedPath));
            assertEquals(metadataDefaultExpectedPath.toString(), metadataDefaultPath);
            assertTrue(Files.exists(metadataDefaultExpectedPath));
        }
    }

    /**
     * Test storeMetadata stores the expected amount of bytes
     */
    @Test
    public void storeMetadata_fileSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            long writtenMetadataFile = Files.size(testMetaDataFile);
            long originalMetadataFie = Files.size(Paths.get(metadataPath));
            assertEquals(writtenMetadataFile, originalMetadataFie);
        }
    }

    /**
     * Test storeMetadata throws exception when metadata is null
     */
    @Test
    public void storeMetadata_metadataNull() {
        for (String pid : testData.pidList) {
            assertThrows(
                IllegalArgumentException.class, () -> fileHashStore.storeMetadata(null, pid, null)
            );
        }
    }

    /**
     * Test storeMetadata throws exception when pid is null
     */
    @Test
    public void storeMetadata_pidNull() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.storeMetadata(metadataStream, null, null);
                metadataStream.close();
            });
        }
    }

    /**
     * Test storeMetadata throws exception when pid is empty
     */
    @Test
    public void storeMetadata_pidEmpty() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.storeMetadata(metadataStream, "", null);
                metadataStream.close();
            });
        }
    }

    /**
     * Test storeMetadata throws exception when pid is empty with spaces
     */
    @Test
    public void storeMetadata_pidEmptySpaces() {
        for (String pid : testData.pidList) {
            assertThrows(IllegalArgumentException.class, () -> {
                String pidFormatted = pid.replace("/", "_");

                // Get test metadata file
                Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

                InputStream metadataStream = Files.newInputStream(testMetaDataFile);

                fileHashStore.storeMetadata(metadataStream, "     ", null);
                metadataStream.close();
            });
        }
    }

    /**
     * Tests that the `storeMetadata()` method can store metadata successfully with multiple threads
     * (3) and does not throw any exceptions. This test uses three futures (threads) that run
     * concurrently, each of which will have to wait for the given `pid` to be released from
     * metadataLockedIds before proceeding to store the given metadata content from its
     * `storeMetadata()` request.
     * 
     * All requests to store the same metadata will be executed, and the existing metadata file will
     * be overwritten by each thread. No exceptions should be encountered during these tests.
     */
    @Test
    public void storeMetadata_metadataLockedIds() throws Exception {
        // Get single test metadata file to "upload"
        String pid = "jtao.1700.1";
        String pidFormatted = pid.replace("/", "_");
        // Get test metadata file
        Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

        // Create a thread pool with 3 threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Submit 3 threads, each calling storeMetadata
        Future<?> future1 = executorService.submit(() -> {
            try {
                String formatId = "https://ns.dataone.org/service/types/v2.0#SystemMetadata";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                metadataStream.close();
                // Calculate absolute path
                String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
                Path metadataPidExpectedPath = fileHashStore.getExpectedPath(
                    pid, "metadata", storeMetadataNamespace
                );
                assertEquals(metadataPath, metadataPidExpectedPath.toString());
            } catch (IOException | NoSuchAlgorithmException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                String formatId = "https://ns.dataone.org/service/types/v2.0#SystemMetadata";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                metadataStream.close();
                // Calculate absolute path
                String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
                Path metadataPidExpectedPath = fileHashStore.getExpectedPath(
                    pid, "metadata", storeMetadataNamespace
                );
                assertEquals(metadataPath, metadataPidExpectedPath.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Future<?> future3 = executorService.submit(() -> {
            try {
                String formatId = "https://ns.dataone.org/service/types/v2.0#SystemMetadata";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataPath = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                metadataStream.close();
                // Calculate absolute path
                String storeMetadataNamespace = fhsProperties.getProperty("storeMetadataNamespace");
                Path metadataPidExpectedPath = fileHashStore.getExpectedPath(
                    pid, "metadata", storeMetadataNamespace
                );
                assertEquals(metadataPath, metadataPidExpectedPath.toString());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });

        // Wait for all tasks to complete and check results
        // .get() on the future ensures that all tasks complete before the test ends
        future1.get();
        future2.get();
        future3.get();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        // Confirm metadata file is written
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        String formatId = fhsProperties.getProperty("storeMetadataNamespace");
        Path metadataCidAbsPath = fileHashStore.getExpectedPath(pid, "metadata", formatId);
        assertTrue(Files.exists(metadataCidAbsPath));

        // Confirm there are only three files in HashStore - 'hashstore.yaml', the metadata file written
        // and the metadata refs file that contains namespaces used
        try (Stream<Path> walk = Files.walk(storePath)) {
            long fileCount = walk.filter(Files::isRegularFile).count();
            assertEquals(fileCount, 2);
        }
    }

    /**
     * Check that retrieveObject returns an InputStream
     */
    @Test
    public void retrieveObject() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Retrieve object
            InputStream objectCidInputStream = fileHashStore.retrieveObject(pid);
            assertNotNull(objectCidInputStream);
            objectCidInputStream.close();
        }
    }

    /**
     * Check that retrieveObject throws exception when there is no object
     * associated with a given pid
     */
    @Test
    public void retrieveObject_pidDoesNotExist() {
        assertThrows(FileNotFoundException.class, () -> {
            fileHashStore.retrieveObject("pid.whose.object.does.not.exist");
        });
    }

    /**
     * Check that retrieveObject throws exception when pid is null
     */
    @Test
    public void retrieveObject_pidNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.retrieveObject(null);
        });
    }

    /**
     * Check that retrieveObject throws exception when pid is empty
     */
    @Test
    public void retrieveObject_pidEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.retrieveObject("");
        });
    }

    /**
     * Check that retrieveObject throws exception when pid is empty spaces
     */
    @Test
    public void retrieveObject_pidEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.retrieveObject("      ");
        });
    }

    /**
     * Check that retrieveObject throws exception when file is not found
     */
    @Test
    public void retrieveObject_pidNotFound() {
        assertThrows(FileNotFoundException.class, () -> {
            fileHashStore.retrieveObject("dou.2023.hs.1");
        });
    }

    /**
     * Check that retrieveObject InputStream content is correct
     */
    @Test
    public void retrieveObject_verifyContent() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Retrieve object
            InputStream objectCidInputStream;
            try {
                objectCidInputStream = fileHashStore.retrieveObject(pid);

            } catch (Exception e) {
                e.printStackTrace();
                throw e;

            }

            // Read content and compare it to the SHA-256 checksum from TestDataHarness
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            try {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = objectCidInputStream.read(buffer)) != -1) {
                    sha256.update(buffer, 0, bytesRead);
                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;

            } finally {
                // Close stream
                objectCidInputStream.close();
            }

            // Get hex digest
            String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
            String sha256DigestFromTestData = testData.pidData.get(pid).get("sha256");
            assertEquals(sha256Digest, sha256DigestFromTestData);
        }
    }

    /**
     * Check that retrieveMetadata returns an InputStream
     */
    @Test
    public void retrieveMetadata() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");

            InputStream metadataCidInputStream = fileHashStore.retrieveMetadata(pid, storeFormatId);
            assertNotNull(metadataCidInputStream);
        }
    }

    /**
     * Check that retrieveMetadata returns an InputStream with overload method
     */
    @Test
    public void retrieveMetadata_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            InputStream metadataCidInputStream = fileHashStore.retrieveMetadata(pid);
            assertNotNull(metadataCidInputStream);
        }
    }

    /**
     * Check that retrieveMetadata throws exception when pid is null
     */
    @Test
    public void retrieveMetadata_pidNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            InputStream pidInputStream = fileHashStore.retrieveMetadata(null, storeFormatId);
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when pid is empty
     */
    @Test
    public void retrieveMetadata_pidEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            InputStream pidInputStream = fileHashStore.retrieveMetadata("", storeFormatId);
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when pid is empty spaces
     */
    @Test
    public void retrieveMetadata_pidEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            InputStream pidInputStream = fileHashStore.retrieveMetadata("      ", storeFormatId);
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when format is null
     */
    @Test
    public void retrieveMetadata_formatNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveMetadata("dou.2023.hs.1", null);
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when format is empty
     */
    @Test
    public void retrieveMetadata_formatEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveMetadata("dou.2023.hs.1", "");
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when format is empty spaces
     */
    @Test
    public void retrieveMetadata_formatEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveMetadata("dou.2023.hs.1", "      ");
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata throws exception when file is not found
     */
    @Test
    public void retrieveMetadata_pidNotFound() {
        assertThrows(FileNotFoundException.class, () -> {
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            InputStream pidInputStream = fileHashStore.retrieveMetadata(
                "dou.2023.hs.1", storeFormatId
            );
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveMetadata InputStream content is correct
     */
    @Test
    public void retrieveMetadata_verifyContent() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");

            // Retrieve object
            InputStream metadataCidInputStream;
            try {
                metadataCidInputStream = fileHashStore.retrieveMetadata(pid, storeFormatId);

            } catch (Exception e) {
                e.printStackTrace();
                throw e;

            }

            // Read content and compare it to the SHA-256 checksum from TestDataHarness
            MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
            try {
                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = metadataCidInputStream.read(buffer)) != -1) {
                    sha256.update(buffer, 0, bytesRead);
                }

            } catch (IOException ioe) {
                ioe.printStackTrace();
                throw ioe;

            } finally {
                // Close stream
                metadataCidInputStream.close();
            }

            // Get hex digest
            String sha256MetadataDigest = DatatypeConverter.printHexBinary(sha256.digest())
                .toLowerCase();
            String sha256MetadataDigestFromTestData = testData.pidData.get(pid).get(
                "metadata_sha256"
            );
            assertEquals(sha256MetadataDigest, sha256MetadataDigestFromTestData);
        }
    }

    /**
     * Confirm that deleteObject throws exceptions when not using HashStoreIdTypes
     */
    @Test
    public void deleteObject_invalidIdTypes() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(
                "PID", "dou.2023.hashstore.1"
            )
        );
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(
                "CID", "dou.2023.hashstore.1"
            )
        );
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(
                "bad.value", "dou.2023.hashstore.1"
            )
        );
    }


    /**
     * Confirm that deleteObject overload method with signature (String pid) deletes objects
     * and all metadata documents.
     */
    @Test
    public void deleteObject_stringPidAll() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Get metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            String testFormatId = "https://test.arcticdata.io/ns";
            String metadataPathString = fileHashStore.storeMetadata(
                metadataStream, pid, testFormatId
            );
            metadataStream.close();
            InputStream metadataStreamTwo = Files.newInputStream(testMetaDataFile);
            String metadataDefaultPathString = fileHashStore.storeMetadata(metadataStreamTwo, pid);
            Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
            Path metadataPath = Paths.get(metadataPathString);
            Path metadataDefaultPath = Paths.get(metadataDefaultPathString);
            metadataStreamTwo.close();

            // Confirm expected documents exist
            assertTrue(Files.exists(metadataPath));
            assertTrue(Files.exists(metadataDefaultPath));
            assertTrue(Files.exists(objCidAbsPath));

            fileHashStore.deleteObject(pid);

            // Check documents have been deleted
            assertFalse(Files.exists(metadataPath));
            assertFalse(Files.exists(metadataDefaultPath));
            assertFalse(Files.exists(objCidAbsPath));
        }
    }


    /**
     * Confirm that deleteObject overload method with signature (String pid) deletes objects
     * and does not throw exceptions if metadata documents do not exist.
     */
    @Test
    public void deleteObject_stringPidNoMetadataDocs() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            // Get metadata file
            Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);

            // Confirm expected documents exist
            assertTrue(Files.exists(objCidAbsPath));

            fileHashStore.deleteObject(pid);

            // Check documents have been deleted
            assertFalse(Files.exists(objCidAbsPath));
        }
    }


    /**
     * Confirm that deleteObject deletes object
     */
    @Test
    public void deleteObject_pidType_objectDeleted() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
            dataStream.close();

            Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
            fileHashStore.deleteObject(fhsDeleteTypePid, pid);

            // Check that file doesn't exist
            assertFalse(Files.exists(objCidAbsPath));

            // Check that parent directories are not deleted
            assertTrue(Files.exists(objCidAbsPath.getParent()));

            // Check that object directory still exists
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path storeObjectPath = storePath.resolve("objects");
            assertTrue(Files.exists(storeObjectPath));
        }
    }

    /**
     * Confirm that deleteObject deletes reference files
     */
    @Test
    public void deleteObject_pidType_referencesDeleted() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();
            String cid = objInfo.getCid();

            // Path objAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
            Path absPathPidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
            Path absPathCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            fileHashStore.deleteObject(fhsDeleteTypePid, pid);
            assertFalse(Files.exists(absPathPidRefsPath));
            assertFalse(Files.exists(absPathCidRefsPath));
        }
    }

    /**
     * Confirm that cid refs file and object do not get deleted when an object has more than one
     * reference (when the client calls 'deleteObject' on a pid that references an object that still
     * has references).
     */
    @Test
    public void deleteObject_pidType_CidRefsFileNotEmptyObjectExistsStill() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();
            String pidExtra = "dou.test" + pid;
            String cid = objInfo.getCid();
            fileHashStore.tagObject(pidExtra, cid);

            Path objCidAbsPath = fileHashStore.getExpectedPath(pid, "object", null);
            Path absPathPidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
            Path absPathCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            fileHashStore.deleteObject(fhsDeleteTypePid, pid);

            assertFalse(Files.exists(absPathPidRefsPath));
            assertTrue(Files.exists(objCidAbsPath));
            assertTrue(Files.exists(absPathCidRefsPath));
        }
    }

    /**
     * Confirm that deleteObject removes an orphan pid reference file when the associated cid refs
     * file does not contain the expected pid.
     */
    @Test
    public void deleteObject_pidType_pidOrphan() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();
            String cid = objInfo.getCid();
            String pidExtra = "dou.test" + pid;
            Path objRealPath = fileHashStore.getExpectedPath(pid, "object", null);

            // Manually change the pid found in the cid refs file
            Path absPathCidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            fileHashStore.updateRefsFile(pidExtra, absPathCidRefsPath, "add");
            // Create an orphaned pid refs file
            fileHashStore.updateRefsFile(pid, absPathCidRefsPath, "remove");

            fileHashStore.deleteObject(fhsDeleteTypePid, pid);

            // Confirm cid refs file still exists
            assertTrue(Files.exists(absPathCidRefsPath));
            // Confirm the original (and now orphaned) pid refs file is deleted
            Path absPathPidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
            assertFalse(Files.exists(absPathPidRefsPath));
            // Confirm the object has not been deleted
            assertTrue(Files.exists(objRealPath));
            // Confirm the cid refs file still exists
            assertTrue(Files.exists(absPathCidRefsPath));
        }
    }

    /**
     * Confirm that deleteObject throws exception when associated pid obj not found
     */
    @Test
    public void deleteObject_pidType_NotFoundPid() {
        assertThrows(
            FileNotFoundException.class, () -> fileHashStore.deleteObject(
                fhsDeleteTypePid, "dou.2023.hashstore.1"
            )
        );
    }

    /**
     * Confirm that deleteObject throws exception when pid is null
     */
    @Test
    public void deleteObject_idNull() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(fhsDeleteTypePid, null)
        );
    }

    /**
     * Confirm that deleteObject throws exception when pid is empty
     */
    @Test
    public void deleteObject_idEmpty() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(fhsDeleteTypePid, "")
        );
    }

    /**
     * Confirm that deleteObject throws exception when pid is empty spaces
     */
    @Test
    public void deleteObject_idEmptySpaces() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.deleteObject(
                fhsDeleteTypePid, "      "
            )
        );
    }

    /**
     * Confirm deleteObject removes pid and cid refs orphan files
     */
    @Test
    public void deleteObject_orphanRefsFiles() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path absPathCidRefsPath = fileHashStore.getExpectedPath(pid, "refs", "pid");
        Path absPathPidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");

        fileHashStore.deleteObject("pid", pid);
        assertFalse(Files.exists(absPathCidRefsPath));
        assertFalse(Files.exists(absPathPidRefsPath));
    }

    /**
     * Confirm deleteObject with idType 'cid' deletes cid object
     */
    @Test
    public void deleteObject_cidType() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();
            String cid = objInfo.getCid();

            fileHashStore.deleteObject(fhsDeleteTypeCid, cid);

            // Get permanent address of the actual cid
            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String actualCid = objInfo.getCid();
            String cidShardString = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, actualCid
            );
            Path objectStoreDirectory = rootDirectory.resolve("objects").resolve(cidShardString);
            assertFalse(Files.exists(objectStoreDirectory));
        }
    }

    /**
     * Confirm deleteObject with idType 'cid' does not delete an object because a cid refs file
     * exists (there are still pids referencing the object)
     */
    @Test
    public void deleteObject_cidType_AndCidRefsExists() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();
            String cid = objInfo.getCid();

            fileHashStore.deleteObject(fhsDeleteTypeCid, cid);

            // Get permanent address of the actual cid
            Path objRealPath = fileHashStore.getExpectedPath(pid, "object", null);
            assertTrue(Files.exists(objRealPath));
            // Confirm cid refs file still exists
            Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
            assertTrue(Files.exists(cidRefsPath));
        }
    }

    /**
     * Test deleteObject synchronization using a Runnable class
     */
    @Test
    public void deleteObject_50Pids_1Obj_viaRunnable() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        List<String> pidModifiedList = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            pidModifiedList.add(pid + ".dou.test." + i);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // Store 50
        for (String pidAdjusted : pidModifiedList) {
            InputStream dataStream = Files.newInputStream(testDataFile);
            HashStoreRunnable
                request = new HashStoreRunnable(fileHashStore, 1, dataStream, pidAdjusted);
            executorService.execute(request);
        }
        // Delete 50
        for (String pidAdjusted : pidModifiedList) {
            HashStoreRunnable
                request = new HashStoreRunnable(fileHashStore, 2, pidAdjusted);
            executorService.execute(request);
        }

        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);

        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        // Check that no objects exist
        List<Path> objects = FileHashStoreUtility.getFilesFromDir(storePath.resolve("objects"));
        assertEquals(0, objects.size());
        // Check that no refs files exist
        List<Path> pidRefFiles = FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs"
                                                                                            + "/pids"));
        assertEquals(0, pidRefFiles.size());
        List<Path> cidRefFiles = FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs"
                                                                                            + "/cids"));
        assertEquals(0, cidRefFiles.size());
    }


    /**
     * Confirm that deleteMetadata deletes metadata and empty sub directories
     */
    @Test
    public void deleteMetadata() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();

            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            fileHashStore.deleteMetadata(pid, storeFormatId);

            // Check that file doesn't exist
            Path metadataCidPath = fileHashStore.getExpectedPath(pid, "metadata", storeFormatId);
            assertFalse(Files.exists(metadataCidPath));

            // Check that parent directories are not deleted
            assertTrue(Files.exists(metadataCidPath.getParent()));

            // Check that metadata directory still exists
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path storeObjectPath = storePath.resolve("metadata");
            assertTrue(Files.exists(storeObjectPath));
        }
    }

    /**
     * Confirm that deleteMetadata deletes all metadata stored for a given pid.
     */
    @Test
    public void deleteMetadata_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");
            String formatIdTwo = "ns.type.2";
            String formatIdThree = "ns.type.3";

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);
            metadataStream.close();
            InputStream metadataStreamTwo = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStreamTwo, pid, formatIdTwo);
            metadataStreamTwo.close();
            InputStream metadataStreamThree = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStreamThree, pid, formatIdThree);
            metadataStreamThree.close();


            fileHashStore.deleteMetadata(pid);

            // Check that file doesn't exist
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            Path metadataCidPath = fileHashStore.getExpectedPath(pid, "metadata", storeFormatId);
            Path metadataCidPathTwo = fileHashStore.getExpectedPath(pid, "metadata", formatIdTwo);
            Path metadataCidPathThree = fileHashStore.getExpectedPath(
                pid, "metadata", formatIdThree
            );

            assertFalse(Files.exists(metadataCidPath));
            assertFalse(Files.exists(metadataCidPathTwo));
            assertFalse(Files.exists(metadataCidPathThree));

            // Check that parent directories are not deleted
            assertTrue(Files.exists(metadataCidPath.getParent()));

            // Check that metadata directory still exists
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path storeObjectPath = storePath.resolve("metadata");
            assertTrue(Files.exists(storeObjectPath));
        }
    }

    /**
     * Confirm that no exceptions are thrown when called to delete metadata
     * that does not exist.
     */
    @Test
    public void deleteMetadata_pidNotFound() throws Exception {
        String formatId = "http://hashstore.tests/types/v1.0";
        fileHashStore.deleteMetadata("dou.2023.hashstore.1", formatId);
    }

    /**
     * Confirm that deleteMetadata throws exception when pid is null
     */
    @Test
    public void deleteMetadata_pidNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            String formatId = "http://hashstore.tests/types/v1.0";
            fileHashStore.deleteMetadata(null, formatId);
        });
    }

    /**
     * Confirm that deleteMetadata throws exception when pid is empty
     */
    @Test
    public void deleteMetadata_pidEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            String formatId = "http://hashstore.tests/types/v1.0";
            fileHashStore.deleteMetadata("", formatId);
        });
    }

    /**
     * Confirm that deleteMetadata throws exception when pid is empty spaces
     */
    @Test
    public void deleteMetadata_pidEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            String formatId = "http://hashstore.tests/types/v1.0";
            fileHashStore.deleteMetadata("      ", formatId);
        });
    }

    /**
     * Confirm that deleteMetadata throws exception when formatId is null
     */
    @Test
    public void deleteMetadata_formatIdNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            String pid = "dou.2023.hashstore.1";
            fileHashStore.deleteMetadata(pid, null);
        });
    }

    /**
     * Confirm that deleteMetadata throws exception when formatId is empty
     */
    @Test
    public void deleteMetadata_formatIdEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            String pid = "dou.2023.hashstore.1";
            fileHashStore.deleteMetadata(pid, "");
        });
    }

    /**
     * Confirm that deleteMetadata throws exception when formatId is empty spaces
     */
    @Test
    public void deleteMetadata_formatIdEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            String pid = "dou.2023.hashstore.1";
            fileHashStore.deleteMetadata(pid, "     ");
        });
    }

    /**
     * Confirm correct checksum/hex digest returned
     */
    @Test
    public void getHexDigest() throws Exception {
        for (String pid : testData.pidList) {
            // Store file first
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            // Then get the checksum
            String pidHexDigest = fileHashStore.getHexDigest(pid, "SHA-256");
            String sha256DigestFromTestData = testData.pidData.get(pid).get("sha256");
            String objSha256Checksum = objInfo.getHexDigests().get("SHA-256");
            assertEquals(pidHexDigest, sha256DigestFromTestData);
            assertEquals(pidHexDigest, objSha256Checksum);
        }
    }

    /**
     * Confirm getHexDigest throws exception when file is not found
     */
    @Test
    public void getHexDigest_pidNotFound() {
        for (String pid : testData.pidList) {
            assertThrows(FileNotFoundException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                fileHashStore.getHexDigest(pidFormatted, "SHA-256");
            });
        }
    }

    /**
     * Confirm getHexDigest throws exception when file is not found
     */
    @Test
    public void getHexDigest_pidNull() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.getHexDigest(null, "SHA-256")
        );
    }

    /**
     * Confirm getHexDigest throws exception when file is not found
     */
    @Test
    public void getHexDigest_pidEmpty() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.getHexDigest("", "SHA-256")
        );
    }

    /**
     * Confirm getHexDigest throws exception when file is not found
     */
    @Test
    public void getHexDigest_pidEmptySpaces() {
        assertThrows(
            IllegalArgumentException.class, () -> fileHashStore.getHexDigest("      ", "SHA-256")
        );
    }

    /**
     * Confirm getHexDigest throws exception when unsupported algorithm supplied
     */
    @Test
    public void getHexDigest_badAlgo() {
        for (String pid : testData.pidList) {
            assertThrows(NoSuchAlgorithmException.class, () -> {
                // Store object first
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);
                dataStream.close();

                fileHashStore.getHexDigest(pid, "BLAKE2S");
            });
        }
    }

    /**
     * Check that findObject returns cid as expected.
     */
    @Test
    public void findObject_cid() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            dataStream.close();

            String cidRetrieved = fileHashStore.findObject(pid);
            assertEquals(cidRetrieved, objInfo.getCid());
        }
    }

    /**
     * Confirm findObject throws exception when cid object does not exist but reference
     * files exist.
     */
    @Test
    public void findObject_refsFileExistButObjectDoesNot() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        assertThrows(OrphanRefsFilesException.class, () -> {
            fileHashStore.findObject(pid);
        });
    }

    /**
     * Confirm that findObject throws OrphanPidRefsFileException exception when
     * pid refs file found but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        Files.delete(cidRefsPath);

        assertThrows(OrphanPidRefsFileException.class, () -> {
            fileHashStore.findObject(pid);
        });
    }


    /**
     * Confirm that findObject throws PidNotFoundInCidRefsFileException exception when
     * pid refs file found but cid refs file is missing.
     */
    @Test
    public void findObject_cidRefsFileMissingPid() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsPath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        fileHashStore.updateRefsFile(pid, cidRefsPath, "remove");

        assertThrows(PidNotFoundInCidRefsFileException.class, () -> {
            fileHashStore.findObject(pid);
        });
    }

    /**
     * Check that exception is thrown when pid refs file doesn't exist
     */
    @Test
    public void findObject_pidNotFound() {
        String pid = "dou.test.1";
        assertThrows(FileNotFoundException.class, () -> {
            fileHashStore.findObject(pid);
        });
    }

}

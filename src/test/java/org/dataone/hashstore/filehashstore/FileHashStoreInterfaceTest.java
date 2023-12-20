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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.PidObjectExistsException;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


/**
 * Test class for FileHashStore HashStoreInterface override methods
 */
public class FileHashStoreInterfaceTest {
    private FileHashStore fileHashStore;
    private Properties fhsProperties;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize FileHashStore before each test to creates tmp directories
     */
    @BeforeEach
    public void initializeFileHashStore() {
        Path rootDirectory = tempFolder.resolve("metacat");

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

            // Check id (content identifier based on the store algorithm)
            String objectCid = testData.pidData.get(pid).get("sha256");
            assertEquals(objectCid, objInfo.getId());
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
            });
        }
    }

    /**
     * Verify that storeObject generates an additional checksum with overloaded method
     */
    @Test
    public void storeObject_additionalAlgorithm_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, "MD2");

            Map<String, String> hexDigests = objInfo.getHexDigests();

            // Validate checksum values
            String md2 = testData.pidData.get(pid).get("md2");
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Verify that storeObject validates checksum with overloaded method
     */
    @Test
    public void storeObject_validateChecksum_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);
            String md2 = testData.pidData.get(pid).get("md2");

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, md2, "MD2");

            Map<String, String> hexDigests = objInfo.getHexDigests();

            // Validate checksum values
            assertEquals(md2, hexDigests.get("MD2"));
        }
    }

    /**
     * Check that store object returns the correct ObjectMetadata size with overloaded method
     */
    @Test
    public void storeObject_objSize_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            long objectSize = Long.parseLong(testData.pidData.get(pid).get("size"));
            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream, pid, objectSize);

            assertEquals(objectSize, objInfo.getSize());
        }
    }

    /**
     * Check that store object executes as expected with only an InputStream (does not create
     * any reference files)
     */
    @Test
    public void storeObject_inputStream_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);

            Map<String, String> hexDigests = objInfo.getHexDigests();
            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");
            String cid = objInfo.getId();

            assertEquals(hexDigests.get(defaultStoreAlgorithm), cid);

            assertThrows(FileNotFoundException.class, () -> {
                fileHashStore.findObject(pid);
            });

            Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
            assertFalse(Files.exists(cidRefsFilePath));
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

        Path objCidAbsPath = fileHashStore.getRealPath(pid, "object", null);
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
        });
    }

    /**
     * Check that store object throws FileAlreadyExists error when storing duplicate object
     */
    @Test
    public void storeObject_duplicate() {
        for (String pid : testData.pidList) {
            assertThrows(PidObjectExistsException.class, () -> {
                String pidFormatted = pid.replace("/", "_");
                Path testDataFile = testData.getTestFile(pidFormatted);

                InputStream dataStream = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

                InputStream dataStreamDup = Files.newInputStream(testDataFile);
                fileHashStore.storeObject(dataStreamDup, pid, null, null, null, -1);
            });
        }
    }

    /**
     * Test that storeObject successfully stores a 1GB file
     * 
     * Note, a 4GB successfully stored in approximately 1m30s
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

        Path objCidAbsPath = fileHashStore.getRealPath(pid, "object", null);
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
     * will encounter an `ExecutionException`. The thread that does not encounter an exception will
     * store the given object, and verifies that the object is stored successfully.
     * 
     * The threads that run into exceptions will encounter a `RunTimeException` or a
     * `PidObjectExistsException`. If a call is made to 'storeObject' for a pid that is already in
     * progress of being stored, a `RunTimeException` will be thrown.
     * 
     * If a call is made to 'storeObject' for a pid that has been stored, the thread will encounter
     * a `PidObjectExistsException` - since `putObject` checks for the existence of a given data
     * object before it attempts to generate a temp file (write to it, generate checksums, etc.).
     * 
     */
    @Test
    public void storeObject_objectLockedIds_FiveThreads() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        // Create a thread pool with 3 threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Submit 3 threads, each calling storeObject
        Future<?> future1 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });
        Future<?> future3 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });
        Future<?> future4 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });
        Future<?> future5 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
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
     * Tests that the `storeObject` method can store an object successfully with two threads. This
     * test uses two futures (threads) that run concurrently, one of which will encounter an
     * `ExecutionException`. The thread that does not encounter an exception will store the given
     * object, and verifies that the object is stored successfully.
     */
    @Test
    public void storeObject_objectLockedIds_TwoThreads() throws Exception {
        // Get single test file to "upload"
        String pid = "jtao.1700.1";
        Path testDataFile = testData.getTestFile(pid);

        // Create a thread pool with 3 threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Submit 3 threads, each calling storeObject
        Future<?> future1 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                InputStream dataStream = Files.newInputStream(testDataFile);
                ObjectMetadata objInfo = fileHashStore.storeObject(
                    dataStream, pid, null, null, null, 0
                );
                if (objInfo != null) {
                    String objCid = objInfo.getId();
                    Path objCidAbsPath = fileHashStore.getRealPath(objCid, "object", null);
                    assertTrue(Files.exists(objCidAbsPath));
                }
            } catch (Exception e) {
                System.out.println(e.getClass());
                e.printStackTrace();
                assertTrue(e instanceof RuntimeException || e instanceof PidObjectExistsException);
            }
        });

        // Wait for all tasks to complete and check results
        // .get() on the future ensures that all tasks complete before the test ends
        future1.get();
        future2.get();
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.MINUTES);
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
            String metadataCid = fileHashStore.storeMetadata(metadataStream, pid, null);

            // Get relative path
            String metadataCidShardString = fileHashStore.getHierarchicalPathString(
                3, 2, metadataCid
            );
            // Get absolute path
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path metadataCidAbsPath = storePath.resolve("metadata/" + metadataCidShardString);

            assertTrue(Files.exists(metadataCidAbsPath));

            long writtenMetadataFile = Files.size(testMetaDataFile);
            long originalMetadataFie = Files.size(metadataCidAbsPath);
            assertEquals(writtenMetadataFile, originalMetadataFie);
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
            String metadataCid = fileHashStore.storeMetadata(metadataStream, pid);

            // Get relative path
            String metadataCidShardString = fileHashStore.getHierarchicalPathString(
                3, 2, metadataCid
            );
            // Get absolute path
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path metadataCidAbsPath = storePath.resolve("metadata/" + metadataCidShardString);

            assertTrue(Files.exists(metadataCidAbsPath));

            long writtenMetadataFile = Files.size(testMetaDataFile);
            long originalMetadataFie = Files.size(metadataCidAbsPath);
            assertEquals(writtenMetadataFile, originalMetadataFie);
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
            String metadataCid = fileHashStore.storeMetadata(metadataStream, pid, null);

            // Get relative path
            String metadataCidShardString = fileHashStore.getHierarchicalPathString(
                3, 2, metadataCid
            );
            // Get absolute path
            Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
            Path metadataCidAbsPath = storePath.resolve("metadata/" + metadataCidShardString);

            long writtenMetadataFile = Files.size(testMetaDataFile);
            long originalMetadataFie = Files.size(metadataCidAbsPath);
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
            });
        }
    }

    /**
     * Tests that the `storeMetadata()` method can store metadata successfully with multiple threads
     * (3). This test uses three futures (threads) that run concurrently, each of which will have to
     * wait for the given `pid` to be released from metadataLockedIds before proceeding to store the
     * given metadata content from its `storeMetadata()` request.
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
        String pidFormatHexDigest =
            "ddf07952ef28efc099d10d8b682480f7d2da60015f5d8873b6e1ea75b4baf689";

        // Create a thread pool with 3 threads
        ExecutorService executorService = Executors.newFixedThreadPool(3);

        // Submit 3 threads, each calling storeMetadata
        Future<?> future1 = executorService.submit(() -> {
            try {
                String formatId = "http://ns.dataone.org/service/types/v2.0";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataCid = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                assertEquals(metadataCid, pidFormatHexDigest);
            } catch (IOException | NoSuchAlgorithmException | InterruptedException e) {
                e.printStackTrace();
            }
        });
        Future<?> future2 = executorService.submit(() -> {
            try {
                String formatId = "http://ns.dataone.org/service/types/v2.0";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataCid = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                assertEquals(metadataCid, pidFormatHexDigest);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        Future<?> future3 = executorService.submit(() -> {
            try {
                String formatId = "http://ns.dataone.org/service/types/v2.0";
                InputStream metadataStream = Files.newInputStream(testMetaDataFile);
                String metadataCid = fileHashStore.storeMetadata(metadataStream, pid, formatId);
                assertEquals(metadataCid, pidFormatHexDigest);
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
        Path metadataCidAbsPath = fileHashStore.getRealPath(pid, "metadata", formatId);
        assertTrue(Files.exists(metadataCidAbsPath));

        // Confirm there are only two files in HashStore - 'hashstore.yaml' and the
        // metadata file written
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

            // Retrieve object
            InputStream objectCidInputStream = fileHashStore.retrieveObject(pid);
            assertNotNull(objectCidInputStream);
        }
    }

    /**
     * Check that retrieveObject throws exception when pid is null
     */
    @Test
    public void retrieveObject_pidNull() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveObject(null);
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveObject throws exception when pid is empty
     */
    @Test
    public void retrieveObject_pidEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveObject("");
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveObject throws exception when pid is empty spaces
     */
    @Test
    public void retrieveObject_pidEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveObject("      ");
            pidInputStream.close();
        });
    }

    /**
     * Check that retrieveObject throws exception when file is not found
     */
    @Test
    public void retrieveObject_pidNotFound() {
        assertThrows(FileNotFoundException.class, () -> {
            InputStream pidInputStream = fileHashStore.retrieveObject("dou.2023.hs.1");
            pidInputStream.close();
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

            }

            // Get hex digest
            String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
            String sha256DigestFromTestData = testData.pidData.get(pid).get("sha256");
            assertEquals(sha256Digest, sha256DigestFromTestData);

            // Close stream
            objectCidInputStream.close();
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

            }

            // Get hex digest
            String sha256MetadataDigest = DatatypeConverter.printHexBinary(sha256.digest())
                .toLowerCase();
            String sha256MetadataDigestFromTestData = testData.pidData.get(pid).get(
                "metadata_sha256"
            );
            assertEquals(sha256MetadataDigest, sha256MetadataDigestFromTestData);

            // Close stream
            metadataCidInputStream.close();
        }
    }

    /**
     * Confirm that deleteObject deletes object and empty subdirectories
     */
    @Test
    public void deleteObject() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            fileHashStore.storeObject(dataStream, pid, null, null, null, -1);

            Path objCidAbsPath = fileHashStore.getRealPath(pid, "object", null);
            fileHashStore.deleteObject(pid);

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
    public void deleteObject_referencesDeleted() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(
                dataStream, pid, null, null, null, -1
            );
            String cid = objInfo.getId();

            // Path objAbsPath = fileHashStore.getRealPath(pid, "object", null);
            Path absPathPidRefsPath = fileHashStore.getRealPath(pid, "refs", "pid");
            Path absPathCidRefsPath = fileHashStore.getRealPath(cid, "refs", "cid");
            fileHashStore.deleteObject(pid);
            assertFalse(Files.exists(absPathPidRefsPath));
            assertFalse(Files.exists(absPathCidRefsPath));
        }
    }

    /**
     * Confirm that deleteObject throws exception when associated pid obj not found
     */
    @Test
    public void deleteObject_pidNotFound() {
        assertThrows(
            FileNotFoundException.class, () -> fileHashStore.deleteObject("dou.2023.hashstore.1")
        );
    }

    /**
     * Confirm that deleteObject throws exception when pid is null
     */
    @Test
    public void deleteObject_pidNull() {
        assertThrows(IllegalArgumentException.class, () -> fileHashStore.deleteObject(null));
    }

    /**
     * Confirm that deleteObject throws exception when pid is empty
     */
    @Test
    public void deleteObject_pidEmpty() {
        assertThrows(IllegalArgumentException.class, () -> fileHashStore.deleteObject(""));
    }

    /**
     * Confirm that deleteObject throws exception when pid is empty spaces
     */
    @Test
    public void deleteObject_pidEmptySpaces() {
        assertThrows(IllegalArgumentException.class, () -> fileHashStore.deleteObject("      "));
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

            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            fileHashStore.deleteMetadata(pid, storeFormatId);

            // Check that file doesn't exist
            Path metadataCidPath = fileHashStore.getRealPath(pid, "metadata", storeFormatId);
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
     * Confirm that deleteMetadata deletes object and empty subdirectories with overload method
     */
    @Test
    public void deleteMetadata_overload() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");

            // Get test metadata file
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            InputStream metadataStream = Files.newInputStream(testMetaDataFile);
            fileHashStore.storeMetadata(metadataStream, pid, null);

            fileHashStore.deleteMetadata(pid);

            // Check that file doesn't exist
            String storeFormatId = (String) fhsProperties.get("storeMetadataNamespace");
            Path metadataCidPath = fileHashStore.getRealPath(pid, "metadata", storeFormatId);
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
     * Confirm that deleteMetadata throws exception when associated pid obj not found
     */
    @Test
    public void deleteMetadata_pidNotFound() {
        assertThrows(FileNotFoundException.class, () -> {
            String formatId = "http://hashstore.tests/types/v1.0";
            fileHashStore.deleteMetadata("dou.2023.hashstore.1", formatId);
        });
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

                fileHashStore.getHexDigest(pid, "BLAKE2S");
            });
        }
    }
}

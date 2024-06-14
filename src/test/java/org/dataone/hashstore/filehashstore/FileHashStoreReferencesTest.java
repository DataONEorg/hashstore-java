package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.filehashstore.FileHashStore.HashStoreIdTypes;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Test class for FileHashStore references related methods
 */
public class FileHashStoreReferencesTest {
    private FileHashStore fileHashStore;
    private Path rootDirectory;
    private Properties fhsProperties;
    private static final TestDataHarness testData = new TestDataHarness();

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
     * Check that tagObject creates reference files
     */
    @Test
    public void tagObject() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] pidRefsFiles = storePath.resolve("refs/pids").toFile().listFiles();
        assertEquals(1, pidRefsFiles.length);
        File[] cidRefsFiles = storePath.resolve("refs/cids").toFile().listFiles();
        assertEquals(1, cidRefsFiles.length);
    }

    /**
     * Check that tagObject writes expected pid refs files
     */
    @Test
    public void tagObject_pidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path pidRefsFilePath = fileHashStore.getExpectedPath(pid, "refs", "pid");
        assertTrue(Files.exists(pidRefsFilePath));

        String retrievedCid = new String(Files.readAllBytes(pidRefsFilePath));
        assertEquals(cid, retrievedCid);
    }

    /**
     * Check that tagObject writes expected cid refs files
     */
    @Test
    public void tagObject_cidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        assertTrue(Files.exists(cidRefsFilePath));

        String retrievedPid = new String(Files.readAllBytes(cidRefsFilePath));
        assertEquals(pid, retrievedPid);
    }

    /**
     * Check that tagObject does not throw exception when pid and cid refs
     * file already exists
     */
    @Test
    public void tagObject_refsFileAlreadyExists() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Should not throw any exceptions, everything is where it's supposed to be.
        fileHashStore.tagObject(pid, cid);
        // Confirm that there is only 1 of each refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] pidRefsFiles = storePath.resolve("refs/pids").toFile().listFiles();
        assertEquals(1, pidRefsFiles.length);
        File[] cidRefsFiles = storePath.resolve("refs/cids").toFile().listFiles();
        assertEquals(1, cidRefsFiles.length);
    }

    /**
     * Check tagObject throws exception when the supplied cid is different from what is
     * found in the pid refs file, and the associated cid refs file from the pid refs file
     * is correctly tagged (everything is where it's expected to be)
     */
    @Test
    public void tagObject_pidRefsFileFound_differentCidRetrieved_cidRefsFileFound()
        throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String existingCid = "987654321fedcba";
        fileHashStore.tagObject(pid, existingCid);

        // This will throw an exception because the pid and cid refs file are in sync
        assertThrows(PidRefsFileExistsException.class, () -> {
            fileHashStore.tagObject(pid, cid);
        });
    }


    /**
     * Check tagObject overwrites a oprhaned pid refs file.
     */
    @Test
    public void tagObject_pidRefsFileFound_differentCidRetrieved_cidRefsFileNotFound()
        throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String cidForOrphanPidRef = "987654321fedcba";

        // Create orphaned pid refs file
        Path absPidRefsPath = fileHashStore.getExpectedPath(
            pid, "refs", HashStoreIdTypes.pid.getName()
        );
        File pidRefsTmpFile = fileHashStore.writeRefsFile(
            cidForOrphanPidRef, HashStoreIdTypes.pid.getName()
        );
        File absPathPidRefsFile = absPidRefsPath.toFile();
        fileHashStore.move(pidRefsTmpFile, absPathPidRefsFile, "refs");

        fileHashStore.tagObject(pid, cid);
        // There should only be 1 of each refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] pidRefsFiles = storePath.resolve("refs/pids").toFile().listFiles();
        assertEquals(1, pidRefsFiles.length);
        File[] cidRefsFiles = storePath.resolve("refs/cids").toFile().listFiles();
        assertEquals(1, cidRefsFiles.length);
    }

    /**
     * Check that tagObject creates a missing cid refs file
     */
    @Test
    public void tagObject_pidRefsFileFound_cidRefsFileNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        // Manually delete the cid refs file
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        Files.delete(cidRefsFilePath);

        fileHashStore.tagObject(pid, cid);
        // Confirm that there is only 1 of each refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] pidRefsFiles = storePath.resolve("refs/pids").toFile().listFiles();
        assertEquals(1, pidRefsFiles.length);
        File[] cidRefsFiles = storePath.resolve("refs/cids").toFile().listFiles();
        assertEquals(1, cidRefsFiles.length);
    }


    /**
     * Check that tagObject creates a pid refs file and updates an existing cid refs file
     */
    @Test
    public void tagObject_pidRefsFileNotFound_cidRefsFileFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        String pidAdditional = "another.pid.2";
        fileHashStore.tagObject(pidAdditional, cid);

        // Confirm missing pid refs file has been created
        Path pidAdditionalRefsFilePath = fileHashStore.getExpectedPath(
            pidAdditional, "refs", "pid"
        );
        assertTrue(Files.exists(pidAdditionalRefsFilePath));

        // Check cid refs file
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        boolean pidFoundInCidRefFiles = fileHashStore.isStringInRefsFile(
            pidAdditional, cidRefsFilePath
        );
        assertTrue(pidFoundInCidRefFiles);

        // There should be 2 pid refs file, and 1 cid refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        File[] pidRefsFiles = storePath.resolve("refs/pids").toFile().listFiles();
        assertEquals(2, pidRefsFiles.length);
        File[] cidRefsFiles = storePath.resolve("refs/cids").toFile().listFiles();
        assertEquals(1, cidRefsFiles.length);
    }


    /**
     * Check that the cid supplied is written into the file given
     */
    @Test
    public void writeRefsFile_content() throws Exception {
        String cidToWrite = "test_cid_123";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");

        String cidRead = new String(Files.readAllBytes(pidRefsTmpFile.toPath()));
        assertEquals(cidRead, cidToWrite);
    }

    /**
     * Check that exception is thrown when incorrect cid in a pid refs file.
     */
    @Test
    public void verifyHashStoreRefFiles_unexpectedCid() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Create a pid refs file with the incorrect cid
        String cidToWrite = "123456789abcdef";
        File pidRefsTmpFile = fileHashStore.writeRefsFile(cidToWrite, "pid");
        Path pidRefsTmpFilePath = pidRefsTmpFile.toPath();

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");

        assertThrows(IOException.class, () -> {
            fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsTmpFilePath, cidRefsFilePath);
        });
    }

    /**
     * Check that exception is thrown when an expected pid is not found in a cid refs file
     */
    @Test
    public void verifyHashStoreRefFiles_pidNotFoundInCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Create a cid refs file with a different pid from the one that is expected
        String pidToWrite = "dou.test.2";
        File cidRefsTmpFile = fileHashStore.writeRefsFile(pidToWrite, "cid");
        Path cidRefsTmpFilePath = cidRefsTmpFile.toPath();

        // Get path of the pid refs file
        Path pidRefsFilePath = fileHashStore.getExpectedPath(pid, "refs", "pid");

        assertThrows(IOException.class, () -> {
            fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath, cidRefsTmpFilePath);
        });
    }

    /**
     * Confirm that cid refs file has been updated successfully
     */
    @Test
    public void updateRefsFile_content() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");

        String pidAdditional = "dou.test.2";
        fileHashStore.updateRefsFile("dou.test.2", cidRefsFilePath, "add");

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        boolean pidAdditional_foundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(pidAdditional)) {
                pidAdditional_foundInCidRefFiles = true;
            }
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertTrue(pidOriginal_foundInCidRefFiles);
        assertTrue(pidAdditional_foundInCidRefFiles);
    }

    /**
     * Check that deleteRefsFile deletes file
     */
    @Test
    public void deleteRefsFile_fileDeleted() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path pidRefsFilePath = fileHashStore.getExpectedPath(pid, "refs", "pid");
        fileHashStore.deleteRefsFile(pidRefsFilePath);

        assertFalse(Files.exists(pidRefsFilePath));
    }

    /**
     * Check that deletePidRefsFile throws exception when there is no file to delete
     */
    @Test
    public void deletePidRefsFile_missingPidRefsFile() {
        String pid = "dou.test.1";

        assertThrows(FileNotFoundException.class, () -> {
            Path pidRefsFilePath = fileHashStore.getExpectedPath(pid, "refs", "pid");
            fileHashStore.deleteRefsFile(pidRefsFilePath);
        });
    }

    /**
     * Check that deleteCidRefsPid deletes pid from its cid refs file
     */
    @Test
    public void deleteCidRefsPid_pidRemoved() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        String pidAdditional = "dou.test.2";
        fileHashStore.tagObject(pidAdditional, cid);

        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");
        fileHashStore.updateRefsFile(pid, cidRefsFilePath, "remove");

        assertFalse(fileHashStore.isStringInRefsFile(pid, cidRefsFilePath));
    }

    /**
     * Check that deleteCidRefsPid removes all pids as expected and leaves an
     * empty file.
     */
    @Test
    public void deleteCidRefsPid_allPidsRemoved() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        String pidAdditional = "dou.test.2";
        fileHashStore.tagObject(pidAdditional, cid);
        Path cidRefsFilePath = fileHashStore.getExpectedPath(cid, "refs", "cid");

        fileHashStore.updateRefsFile(pid, cidRefsFilePath, "remove");
        fileHashStore.updateRefsFile(pidAdditional, cidRefsFilePath, "remove");

        assertTrue(Files.exists(cidRefsFilePath));
        assertEquals(0, Files.size(cidRefsFilePath));
    }

    /**
     * Check that verifyObject does not throw exception with matching values
     */
    @Test
    public void verifyObject_correctValues() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("sha256");
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            fileHashStore.verifyObject(
                objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize
            );
        }
    }

    /**
     * Check that verifyObject throws exception when non-matching size value provided
     */
    @Test
    public void verifyObject_mismatchedValuesBadSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("sha256");
            long expectedSize = 123456789;

            assertThrows(NonMatchingObjSizeException.class, () -> {
                fileHashStore.verifyObject(
                    objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize
                );
            });
        }
    }

    /**
     * Check that verifyObject throws exception with non-matching checksum value
     */
    @Test
    public void verifyObject_mismatchedValuesObjectDeleted() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = "intentionallyWrongValue";
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            assertThrows(NonMatchingChecksumException.class, () -> {
                fileHashStore.verifyObject(
                    objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize
                );
            });

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String actualCid = objInfo.getCid();
            String cidShardString = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, actualCid
            );
            Path objectStoreDirectory = rootDirectory.resolve("objects").resolve(cidShardString);
            assertTrue(Files.exists(objectStoreDirectory));

        }
    }
}

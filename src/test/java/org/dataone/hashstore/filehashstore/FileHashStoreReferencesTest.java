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
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
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
     * Check that tagObject writes expected pid refs files
     */
    @Test
    public void tagObject_pidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path pidRefsFilePath = fileHashStore.getRealPath(pid, "refs", "pid");
        assertTrue(Files.exists(pidRefsFilePath));
    }

    /**
     * Check that tagObject writes expected cid refs files
     */
    @Test
    public void tagObject_cidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
        assertTrue(Files.exists(cidRefsFilePath));
    }

    /**
     * Check that tagObject throws exception when pid refs file already exists
     */
    @Test
    public void tagObject_pidRefsFileExists() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        assertThrows(PidRefsFileExistsException.class, () -> {
            fileHashStore.tagObject(pid, cid);
        });

    }

    /**
     * Check that tagObject creates a pid refs file and updates an existing cid refs file
     */
    @Test
    public void tagObject_cidRefsFileExists() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        String pidAdditional = "another.pid.2";
        fileHashStore.tagObject(pidAdditional, cid);

        Path pidRefsFilePath = fileHashStore.getRealPath(pid, "refs", "pid");
        assertTrue(Files.exists(pidRefsFilePath));


        // Check cid refs file
        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
        boolean pidFoundInCidRefFiles = fileHashStore.isPidInCidRefsFile(
            pidAdditional, cidRefsFilePath
        );
        assertTrue(pidFoundInCidRefFiles);
    }

    /**
     * Check that tagObject creates pid refs file when pid already exists in cid refs file
     */
    @Test
    public void tagObject_pidExistsInCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";

        File cidRefsTmpFile = fileHashStore.writeCidRefsFile(pid);
        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
        fileHashStore.move(cidRefsTmpFile, cidRefsFilePath.toFile(), "refs");

        fileHashStore.tagObject(pid, cid);

        Path pidRefsFilePath = fileHashStore.getRealPath(pid, "refs", "pid");
        assertTrue(Files.exists(pidRefsFilePath));

        // Confirm that cid refs file only has 1 line
        List<String> lines = Files.readAllLines(cidRefsFilePath);
        int numberOfLines = lines.size();
        assertEquals(numberOfLines, 1);

    }

    /**
     * Confirm expected cid is returned
     */
    @Test
    public void findObject_content() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        String cidRetrieved = fileHashStore.findObject(pid);

        assertEquals(cid, cidRetrieved);
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

    /**
     * Check that the cid supplied is written into the file given
     */
    @Test
    public void writePidRefsFile_content() throws Exception {
        String cidToWrite = "test_cid_123";
        File pidRefsTmpFile = fileHashStore.writePidRefsFile(cidToWrite);

        String cidRead = new String(Files.readAllBytes(pidRefsTmpFile.toPath()));
        assertEquals(cidRead, cidToWrite);
    }

    /**
     * Check that the pid supplied is written into the file given with a new line
     */
    @Test
    public void writeCidRefsFile_content() throws Exception {
        String pidToWrite = "dou.test.123";
        File cidRefsTmpFile = fileHashStore.writeCidRefsFile(pidToWrite);

        String pidRead = new String(Files.readAllBytes(cidRefsTmpFile.toPath()));
        assertEquals(pidRead, pidToWrite + "\n");
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
        File pidRefsTmpFile = fileHashStore.writePidRefsFile(cidToWrite);
        Path pidRefsTmpFilePath = pidRefsTmpFile.toPath();

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");

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
        String cidToWrite = "dou.test.2";
        File cidRefsTmpFile = fileHashStore.writeCidRefsFile(cidToWrite);
        Path cidRefsTmpFilePath = cidRefsTmpFile.toPath();

        // Get path of the pid refs file
        Path pidRefsFilePath = fileHashStore.getRealPath(pid, "refs", "pid");

        assertThrows(IOException.class, () -> {
            fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath, cidRefsTmpFilePath);
        });
    }

    /**
     * Confirm that cid refs file has been updated successfully
     */
    @Test
    public void updateCidRefsFiles_content() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");

        String pidAdditional = "dou.test.2";
        fileHashStore.updateCidRefsFiles("dou.test.2", cidRefsFilePath);

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
     * Check that deletePidRefsFile deletes file
     */
    @Test
    public void deletePidRefsFile_fileDeleted() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        fileHashStore.deletePidRefsFile(pid);

        Path pidRefsFilePath = fileHashStore.getRealPath(pid, "refs", "pid");
        assertFalse(Files.exists(pidRefsFilePath));
    }

    /**
     * Check that deletePidRefsFile throws exception when there is no file to delete
     */
    @Test
    public void deletePidRefsFile_missingPidRefsFile() throws Exception {
        String pid = "dou.test.1";

        assertThrows(FileNotFoundException.class, () -> {
            fileHashStore.deletePidRefsFile(pid);
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

        fileHashStore.deleteCidRefsPid(pid, cid);

        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
        assertFalse(fileHashStore.isPidInCidRefsFile(pid, cidRefsFilePath));
    }

    /**
     * Check that deleteCidRefsPid throws exception when there is no file to delete the pid from
     */
    @Test
    public void deleteCidRefsPid_missingCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abc123456789";

        assertThrows(FileNotFoundException.class, () -> {
            fileHashStore.deleteCidRefsPid(pid, cid);
        });
    }

    /**
     * Check that deleteCidRefsPid throws exception when there is no file to delete the pid from
     */
    @Test
    public void deleteCidRefsPid_pidNotFoundInCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        assertThrows(IllegalArgumentException.class, () -> {
            fileHashStore.deleteCidRefsPid("bad.pid", cid);
        });
    }

    /**
     * Check that deleteCidRefsFile still exists if called and cid refs file is not empty
     */
    @Test
    public void deleteCidRefsFile_cidRefsFileNotEmpty() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        String pidAdditional = "dou.test.2";
        fileHashStore.tagObject(pidAdditional, cid);

        fileHashStore.deleteCidRefsPid(pid, cid);
        fileHashStore.deleteCidRefsFile(cid);

        Path cidRefsFilePath = fileHashStore.getRealPath(cid, "refs", "cid");
        assertTrue(Files.exists(cidRefsFilePath));
    }

    /**
     * Check that verifyObject verifies with good values
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
     * Check that verifyObject verifies with good values
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

            assertThrows(IllegalArgumentException.class, () -> {
                fileHashStore.verifyObject(
                    objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize
                );
            });
        }
    }

    /**
     * Check that verifyObject deletes file when there is a mismatch
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

            assertThrows(IllegalArgumentException.class, () -> {
                fileHashStore.verifyObject(
                    objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize
                );
            });

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            String actualCid = objInfo.getId();
            String cidShardString = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, actualCid
            );
            Path objectStoreDirectory = rootDirectory.resolve("objects").resolve(cidShardString);
            assertFalse(Files.exists(objectStoreDirectory));

        }
    }
}
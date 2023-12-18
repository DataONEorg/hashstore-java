package org.dataone.hashstore.filehashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Properties;

import org.dataone.hashstore.exceptions.PidExistsInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
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

    /**
     * Utility method to get absolute path of a given object
     */
    public Path getObjectAbsPath(String id, String entity) {
        int shardDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
        int shardWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
        // Get relative path
        String objCidShardString = fileHashStore.getHierarchicalPathString(
            shardDepth, shardWidth, id
        );
        // Get absolute path
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));

        return storePath.resolve(entity).resolve(objCidShardString);
    }

    /**
     * Check that tagObject writes expected pid refs files
     */
    @Test
    public void tagObject_pidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        String pidAddress = fileHashStore.getPidHexDigest(
            pid, fhsProperties.getProperty("storeAlgorithm")
        );
        Path pidRefsFilePath = getObjectAbsPath(pidAddress, "refs/pid");
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

        Path cidRefsFilePath = getObjectAbsPath(cid, "refs/cid");
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

        String pidAddress = fileHashStore.getPidHexDigest(
            pid, fhsProperties.getProperty("storeAlgorithm")
        );
        Path pidRefsFilePath = getObjectAbsPath(pidAddress, "refs/pid");
        assertTrue(Files.exists(pidRefsFilePath));


        // Check cid refs file
        Path cidRefsFilePath = getObjectAbsPath(cid, "refs/cid");
        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidFoundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(pidAdditional)) {
                pidFoundInCidRefFiles = true;
            }
        }
        assertTrue(pidFoundInCidRefFiles);
    }

    /**
     * Check that tagObject throws an exception when calling to write a pid into a cid refs
     * file that already contains the pid
     */
    @Test
    public void tagObject_pidExistsInCidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";

        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        fileHashStore.writeCidRefsFile(refsTmpFile, pid);
        Path cidRefsFilePath = getObjectAbsPath(cid, "refs/cid");
        fileHashStore.move(refsTmpFile, cidRefsFilePath.toFile(), "refs");

        assertThrows(PidExistsInCidRefsFileException.class, () -> {
            fileHashStore.tagObject(pid, cid);
        });
    }

    /**
     * Check that the cid supplied is written into the file given
     */
    @Test
    public void writePidRefsFile_content() throws Exception {
        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        String cidToWrite = "test_cid_123";
        fileHashStore.writePidRefsFile(refsTmpFile, cidToWrite);

        String cidRead = new String(Files.readAllBytes(refsTmpFile.toPath()));
        assertEquals(cidRead, cidToWrite);

    }

    /**
     * Check that the pid supplied is written into the file given with a new line
     */
    @Test
    public void writeCidRefsFile_content() throws Exception {
        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        String pidToWrite = "dou.test.123";
        fileHashStore.writeCidRefsFile(refsTmpFile, pidToWrite);

        String pidRead = new String(Files.readAllBytes(refsTmpFile.toPath()));
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
        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        String cidToWrite = "dou.test.123";
        fileHashStore.writePidRefsFile(refsTmpFile, cidToWrite);
        Path refsTmpFileAbsPath = refsTmpFileDirectory.resolve(refsTmpFile.getName());

        // Get path of the cid refs file
        Path cidRefsFilePath = getObjectAbsPath(cid, "refs/cid");

        assertThrows(IOException.class, () -> {
            fileHashStore.verifyHashStoreRefsFiles(pid, cid, refsTmpFileAbsPath, cidRefsFilePath);
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

        // Create a cid refs file with the incorrect pid
        Path refsTmpFileDirectory = rootDirectory.resolve("refs/tmp");
        File refsTmpFile = fileHashStore.generateTmpFile("tmp", refsTmpFileDirectory);
        String cidToWrite = "dou.test.2";
        fileHashStore.writeCidRefsFile(refsTmpFile, cidToWrite);
        Path refsTmpFileAbsPath = refsTmpFileDirectory.resolve(refsTmpFile.getName());

        // Get path of the pid refs file
        String pidAddress = fileHashStore.getPidHexDigest(
            pid, fhsProperties.getProperty("storeAlgorithm")
        );
        Path pidRefsFilePath = getObjectAbsPath(pidAddress, "refs/pid");

        assertThrows(IOException.class, () -> {
            fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath, refsTmpFileAbsPath);
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
        Path cidRefsFilePath = getObjectAbsPath(cid, "refs/cid");

        String pidAdditional = "dou.test.2";
        fileHashStore.updateCidRefsFiles("dou.test.2", cidRefsFilePath);

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidFoundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(pidAdditional)) {
                pidFoundInCidRefFiles = true;
            }
        }
        assertTrue(pidFoundInCidRefFiles);
    }
}

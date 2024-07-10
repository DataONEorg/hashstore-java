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
import org.dataone.hashstore.exceptions.CidNotFoundInPidRefsFileException;
import org.dataone.hashstore.exceptions.HashStoreRefsAlreadyExistException;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.exceptions.UnsupportedHashAlgorithmException;
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
    private Properties fhsProperties;
    private static final TestDataHarness testData = new TestDataHarness();

    /**
     * Initialize FileHashStore before each test to creates tmp directories
     */
    @BeforeEach
    public void initializeFileHashStore() {
        Path rootDirectory = tempFolder.resolve("hashstore");

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
     * Check tagObject does not throw exception when creating a fresh set
     * of reference files
     */
    @Test
    public void tagObject() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
    }

    /**
     * Check that tagObject successfully tags a cid refs file that already exists
     */
    @Test
    public void tagObject_cidRefsAlreadyExists() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        String pidTwo = "dou.test.2";
        fileHashStore.tagObject(pidTwo, cid);

        // Confirm number of ref files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(2, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that tagObject throws HashStoreRefsAlreadyExistException exception when pid and cid
     * refs file already exists (duplicate tag request)
     */
    @Test
    public void tagObject_HashStoreRefsAlreadyExistException() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // This exception only needs to be re-raised
        assertThrows(
            HashStoreRefsAlreadyExistException.class, () -> fileHashStore.tagObject(pid, cid));

        // Confirm there are only 1 of each ref files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that tagObject throws PidRefsFileExistsException when called to tag a 'pid'
     * that is already referencing another 'cid'
     */
    @Test
    public void tagObject_PidRefsFileExistsException() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // This exception only needs to be re-raised
        assertThrows(
            PidRefsFileExistsException.class, () -> fileHashStore.tagObject(pid, "another.cid"));

        // Confirm there are only 1 of each ref files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that unTagObject deletes reference files
     */
    @Test
    public void unTagObject() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        fileHashStore.unTagObject(pid, cid);

        // Confirm refs files do not exist
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
        assertFalse(Files.exists(absCidRefsPath));
        assertFalse(Files.exists(absPidRefsPath));
    }

    /**
     * Check that unTagObject deletes pid refs file for a cid that is referenced by
     * multiple pids, and that the cid refs file is not deleted.
     */
    @Test
    public void unTagObject_cidWithMultiplePidReferences() throws Exception {
        String pid = "dou.test.1";
        String pidTwo = "dou.test.2";
        String pidThree = "dou.test.3";
        String pidFour = "dou.test.4";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        fileHashStore.tagObject(pidTwo, cid);
        fileHashStore.tagObject(pidThree, cid);
        fileHashStore.tagObject(pidFour, cid);

        fileHashStore.unTagObject(pid, cid);

        // Confirm refs files state
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());

        assertFalse(Files.exists(absPidRefsPath));
        assertTrue(Files.exists(absCidRefsPath));

        // Confirm number of reference files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(3, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that unTagObject deletes an orphaned pid refs file (there is no cid refs file)
     */
    @Test
    public void unTagObject_orphanPidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Delete cid refs file to create orphaned pid refs file
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        Files.delete(absCidRefsPath);
        assertFalse(Files.exists(absCidRefsPath));

        fileHashStore.unTagObject(pid, cid);

        // Confirm pid refs is deleted
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
        assertFalse(Files.exists(absPidRefsPath));

        // Confirm number of reference files
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(0, pidRefsFiles.size());
        assertEquals(0, cidRefsFiles.size());
    }

    /**
     * Check that unTagObject does not throw exception when a pid refs file and cid refs file
     * does not exist
     */
    @Test
    public void unTagObject_missingRefsFiles() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";

        fileHashStore.unTagObject(pid, cid);
    }

    /**
     * Check that unTagObject does not throw exception when a pid refs file and cid refs file
     * does not exist
     */
    @Test
    public void unTagObject_missingPidRefsFile() throws Exception {
        String pid = "dou.test.1";
        String pidTwo = "dou.test.2";
        String pidThree = "dou.test.3";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);
        fileHashStore.tagObject(pidTwo, cid);
        fileHashStore.tagObject(pidThree, cid);

        // Delete pid refs to create scenario
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
        Files.delete(absPidRefsPath);
        assertFalse(Files.exists(absPidRefsPath));

        fileHashStore.unTagObject(pid, cid);

        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        assertFalse(fileHashStore.isStringInRefsFile(pid, absCidRefsPath));
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
     * Check that storeHashStoreRefsFiles creates reference files
     */
    @Test
    public void storeHashStoreRefsFiles() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        // Confirm refs files exist
        Path absCidRefsPath =
            fileHashStore.getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());

        assertTrue(Files.exists(absCidRefsPath));
        assertTrue(Files.exists(absPidRefsPath));

        // Confirm no additional files were created
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that storeHashStoreRefsFiles writes expected pid refs files and that the content
     * is correct
     */
    @Test
    public void storeHashStoreRefsFiles_pidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        Path pidRefsFilePath = fileHashStore.getHashStoreRefsPath(pid, "pid");
        assertTrue(Files.exists(pidRefsFilePath));

        String retrievedCid = new String(Files.readAllBytes(pidRefsFilePath));
        assertEquals(cid, retrievedCid);
    }

    /**
     * Check that storeHashStoreRefsFiles writes expected cid refs files and that the content
     * is correct
     */
    @Test
    public void storeHashStoreRefsFiles_cidRefsFileContent() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        assertTrue(Files.exists(cidRefsFilePath));

        String retrievedPid = new String(Files.readAllBytes(cidRefsFilePath));
        assertEquals(pid, retrievedPid);
    }

    /**
     * Check that storeHashStoreRefsFiles throws HashStoreRefsAlreadyExistException
     * when refs files already exist
     */
    @Test
    public void storeHashStoreRefsFiles_HashStoreRefsAlreadyExistException() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        assertThrows(
            HashStoreRefsAlreadyExistException.class,
            () -> fileHashStore.storeHashStoreRefsFiles(pid, cid));

        // Confirm that there is only 1 of each ref file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check storeHashStoreRefsFiles throws exception when the supplied cid is different from what
     * is found in the pid refs file, and the associated cid refs file from the pid refs file
     * is correctly tagged (everything is where it's expected to be)
     */
    @Test
    public void storeHashStoreRefsFiles_PidRefsFileExistsException()
        throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String existingCid = "987654321fedcba";
        fileHashStore.storeHashStoreRefsFiles(pid, existingCid);

        // This will throw an exception because the pid and cid refs file are in sync
        assertThrows(
            PidRefsFileExistsException.class,
            () -> fileHashStore.storeHashStoreRefsFiles(pid, cid));
    }

    /**
     * Check storeHashStoreRefsFiles overwrites an orphaned pid refs file - the 'cid' that it
     * references does not exist (does not have a cid refs file)
     */
    @Test
    public void storeHashStoreRefsFiles_pidRefsOrphanedFile()
        throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        String cidForOrphanPidRef = "987654321fedcba";

        // Create orphaned pid refs file
        Path absPidRefsPath =
            fileHashStore.getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
        File pidRefsTmpFile = fileHashStore.writeRefsFile(
            cidForOrphanPidRef, HashStoreIdTypes.pid.getName()
        );
        File absPathPidRefsFile = absPidRefsPath.toFile();
        fileHashStore.move(pidRefsTmpFile, absPathPidRefsFile, "refs");

        fileHashStore.storeHashStoreRefsFiles(pid, cid);
        // There should only be 1 of each ref file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(1, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
    }

    /**
     * Check that storeHashStoreRefsFiles creates a pid refs file and updates an existing cid refs
     * file
     */
    @Test
    public void storeHashStoreRefsFiles_updateExistingRefsFile() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.storeHashStoreRefsFiles(pid, cid);

        String pidAdditional = "another.pid.2";
        fileHashStore.storeHashStoreRefsFiles(pidAdditional, cid);

        // Confirm missing pid refs file has been created
        Path pidAdditionalRefsFilePath = fileHashStore.getHashStoreRefsPath(pidAdditional, "pid");
        assertTrue(Files.exists(pidAdditionalRefsFilePath));

        // Check cid refs file
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        boolean pidFoundInCidRefFiles = fileHashStore.isStringInRefsFile(
            pidAdditional, cidRefsFilePath
        );
        assertTrue(pidFoundInCidRefFiles);

        // There should be 2 pid refs file, and 1 cid refs file
        Path storePath = Paths.get(fhsProperties.getProperty("storePath"));
        List<Path> pidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/pids"));
        List<Path> cidRefsFiles =
            FileHashStoreUtility.getFilesFromDir(storePath.resolve("refs" + "/cids"));

        assertEquals(2, pidRefsFiles.size());
        assertEquals(1, cidRefsFiles.size());
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
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");

        assertThrows(
            CidNotFoundInPidRefsFileException.class,
            () -> fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsTmpFilePath,
                                                         cidRefsFilePath));
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
        Path pidRefsFilePath = fileHashStore.getHashStoreRefsPath(pid, "pid");

        assertThrows(
            PidNotFoundInCidRefsFileException.class,
            () -> fileHashStore.verifyHashStoreRefsFiles(pid, cid, pidRefsFilePath,
                                                         cidRefsTmpFilePath));
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
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");

        String pidAdditional = "dou.test.2";
        fileHashStore.updateRefsFile(pidAdditional, cidRefsFilePath, "add");

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
     * Confirm that updateRefsFile does not throw any exception if called to remove a value
     * that is not found in a cid refs file.
     */
    @Test
    public void updateRefsFile_cidRefsPidNotFound() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        fileHashStore.updateRefsFile("dou.test.2", cidRefsFilePath, "remove");

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        int pidsFound = 0;
        for (String line : lines) {
            pidsFound++;
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertTrue(pidOriginal_foundInCidRefFiles);
        assertEquals(1, pidsFound);
    }

    /**
     * Confirm that updateRefsFile does not throw any exception if called to remove a value
     * from a cid refs file that is empty
     */
    @Test
    public void updateRefsFile_cidRefsEmpty() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        // Get path of the cid refs file
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");
        fileHashStore.updateRefsFile(pid, cidRefsFilePath, "remove");

        List<String> lines = Files.readAllLines(cidRefsFilePath);
        boolean pidOriginal_foundInCidRefFiles = false;
        int pidsFound = 0;
        for (String line : lines) {
            pidsFound++;
            if (line.equals(pid)) {
                pidOriginal_foundInCidRefFiles = true;
            }
        }
        assertFalse(pidOriginal_foundInCidRefFiles);
        assertEquals(0, pidsFound);

        // Confirm that no exception is thrown and that the cid refs still exists
        fileHashStore.updateRefsFile(pid, cidRefsFilePath, "remove");
        assertTrue(Files.exists(cidRefsFilePath));
    }

    /**
     * Check that deleteRefsFile deletes file
     */
    @Test
    public void deleteRefsFile_fileDeleted() throws Exception {
        String pid = "dou.test.1";
        String cid = "abcdef123456789";
        fileHashStore.tagObject(pid, cid);

        Path pidRefsFilePath = fileHashStore.getHashStoreRefsPath(pid, "pid");
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
            Path pidRefsFilePath = fileHashStore.getHashStoreRefsPath(pid, "pid");
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

        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");
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
        Path cidRefsFilePath = fileHashStore.getHashStoreRefsPath(cid, "cid");

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
            dataStream.close();

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("sha256");
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            fileHashStore.verifyObject(
                objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize, true
            );

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertTrue(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject calculates and verifies a checksum with a supported algorithm that is
     * not included in the default list
     */
    @Test
    public void verifyObject_supportedAlgoNotInDefaultList() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("md2");
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            fileHashStore.verifyObject(
                objInfo, expectedChecksum, "MD2", expectedSize, true
            );

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertTrue(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject calculates throws exception when given a checksumAlgorithm that is
     * not supported
     */
    @Test
    public void verifyObject_unsupportedAlgo() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            assertThrows(
                UnsupportedHashAlgorithmException.class,
                () -> fileHashStore.verifyObject(objInfo, "ValueNotRelevant", "BLAKE2S", 1000,
                                                 false));

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertTrue(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject throws exception when non-matching size value provided
     */
    @Test
    public void verifyObject_mismatchedSize() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("sha256");
            long expectedSize = 123456789;

            assertThrows(
                NonMatchingObjSizeException.class,
                () -> fileHashStore.verifyObject(objInfo, expectedChecksum, defaultStoreAlgorithm,
                                                 expectedSize, false));

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertTrue(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject throws exception with non-matching checksum value
     */
    @Test
    public void verifyObject_mismatchedChecksum() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = "intentionallyWrongValue";
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            assertThrows(
                NonMatchingChecksumException.class,
                () -> fileHashStore.verifyObject(objInfo, expectedChecksum, defaultStoreAlgorithm,
                                                 expectedSize, false));

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertTrue(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject throws exception when non-matching size value provided
     */
    @Test
    public void verifyObject_mismatchedSize_deleteInvalidObject_true() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = testData.pidData.get(pid).get("sha256");
            long expectedSize = 123456789;

            assertThrows(
                NonMatchingObjSizeException.class,
                () -> fileHashStore.verifyObject(objInfo, expectedChecksum, defaultStoreAlgorithm,
                                                 expectedSize, true));


            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertFalse(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }

    /**
     * Check that verifyObject throws exception with non-matching checksum value
     */
    @Test
    public void verifyObject_mismatchedChecksum_deleteInvalidObject_true() throws Exception {
        for (String pid : testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            InputStream dataStream = Files.newInputStream(testDataFile);
            ObjectMetadata objInfo = fileHashStore.storeObject(dataStream);
            dataStream.close();

            String defaultStoreAlgorithm = fhsProperties.getProperty("storeAlgorithm");

            // Get verifyObject args
            String expectedChecksum = "intentionallyWrongValue";
            long expectedSize = Long.parseLong(testData.pidData.get(pid).get("size"));

            assertThrows(NonMatchingChecksumException.class, () -> fileHashStore.verifyObject(
                objInfo, expectedChecksum, defaultStoreAlgorithm, expectedSize, true
            ));

            int storeDepth = Integer.parseInt(fhsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(fhsProperties.getProperty("storeWidth"));
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                storeDepth, storeWidth, objInfo.getCid()
            );
            // Real path to the data object
            assertFalse(Files.exists(Paths.get(fhsProperties.getProperty("storePath")).resolve(
                "objects").resolve(objRelativePath)));
        }
    }
}

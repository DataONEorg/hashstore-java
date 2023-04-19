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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

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
            HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null, null);

            InputStream dataStreamDup = new FileInputStream(testDataFile);
            HashAddress objInfoDup = hsj.storeObject(pid, dataStreamDup, null, null, null);
        }
    }

}

package org.dataone.hashstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;

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
     * Check that storeDirectory is created
     */
    @Test
    public void testHashStoreConstructor() {
        Path checkStoreObjPath = Paths.get(this.rootStringFull).resolve("objects");
        assertTrue(Files.exists(checkStoreObjPath));

        Path checkStoreObjTmpPath = Paths.get(this.rootStringFull).resolve("objects/tmp");
        assertTrue(Files.exists(checkStoreObjTmpPath));
    }

    /**
     * Check that store object returns the correct HashAddress object info
     */
    @Test
    public void testStoreObject() {
        for (String pid : this.testData.pidList) {
            String pidFormatted = pid.replace("/", "_");
            Path testdataDirectory = Paths.get("src/test/java/org/dataone/hashstore",
                    "testdata", pidFormatted);
            String testdataAbsolutePath = testdataDirectory.toFile().getAbsolutePath();
            File testDataFile = new File(testdataAbsolutePath);

            try {
                InputStream dataStream = new FileInputStream(testDataFile);
                HashAddress objInfo = hsj.storeObject(pid, dataStream, null, null);

                // Check id (sha-256 hex digest of the ab_id, aka s_cid)
                String objAuthorityId = this.testData.pidData.get(pid).get("s_cid");
                assertEquals(objAuthorityId, objInfo.getId());

                // Check relative path
                String objRelPath = this.hsil.shard(3, 2, objAuthorityId);
                assertEquals(objRelPath, objInfo.getRelPath());

                // Check absolute path
                File objAbsPath = new File(objInfo.getAbsPath());
                assertTrue(objAbsPath.exists());

                // Check duplicate status
                assertFalse(objInfo.getIsDuplicate());

            } catch (NoSuchAlgorithmException e) {
                fail("NoSuchAlgorithmExceptionJava: " + e.getMessage());
            } catch (IOException e) {
                fail("IOException: " + e.getMessage());
            }
        }
    }
}

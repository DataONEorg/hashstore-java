package org.dataone.hashstore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

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
}

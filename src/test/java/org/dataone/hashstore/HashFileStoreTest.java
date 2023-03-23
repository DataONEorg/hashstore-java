package org.dataone.hashstore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * Test class for HashFileStore
 */
public class HashFileStoreTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Check that directory has been created for object store
     */
    @Test
    public void checkCreateDirectory() {
        Path rootDirectory = tempFolder.getRoot().toPath();
        String rootString = rootDirectory.toString();
        String rootStringFull = rootString + "metacat/objects";
        new HashFileStore(3, 2, "sha256", rootStringFull);
        Path checkPath = Paths.get(rootStringFull);
        assertTrue(Files.exists(checkPath));
    }
}

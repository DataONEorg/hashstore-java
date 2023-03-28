package org.dataone.hashstore;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.dataone.hashstore.hashfs.HashUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test class for HashStore utility methods
 */
public class HashUtilTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    /**
     * Confirm that a temporary file has been generated.
     */
    @Test
    public void testCreateTemporaryFile() {
        String prefix = "testfile";
        File directory = tempFolder.getRoot();
        File newFile = null;
        try {
            HashUtil hsil = new HashUtil();
            newFile = hsil.generateTmpFile(prefix, directory);
        } catch (IOException e) {
            fail("IOException encountered: " + e.getMessage());
        }
        assertTrue(newFile.exists());
    }
}
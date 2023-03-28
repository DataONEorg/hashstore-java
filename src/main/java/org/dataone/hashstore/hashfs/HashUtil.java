package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.IOException;

/**
 * HashUtil provides utility methods for HashFileStore
 */
public class HashUtil {
    /**
     * Create a temporary new file
     * 
     * @param prefix
     * @return
     * @throws IOException
     */
    public File generateTmpFile(String prefix, File directory) throws IOException {
        String newPrefix = prefix + "-" + System.currentTimeMillis();
        String suffix = null;
        File newFile = null;
        try {
            newFile = File.createTempFile(newPrefix, suffix, directory);
        } catch (Exception e) {
            // try again if the first time fails
            newFile = File.createTempFile(newPrefix, suffix, directory);
            // TODO: Log Exception e
        }
        // TODO: Log - newFile.getCanonicalPath());
        return newFile;
    }
}

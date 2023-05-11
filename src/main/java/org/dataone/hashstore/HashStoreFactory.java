package org.dataone.hashstore;

import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.filehashstore.FileHashStore;

/**
 * HashStoreFactory is a factory class that generates HashStore, a
 * content-addressable file management system that utilizes a persistent
 * identifier (PID) in the form of a hex digest value to address files. The
 * system stores files in a file store and provides an API for interacting with
 * the store. HashStore storage classes (like `FileHashStore`) must implement
 * the HashStoreInterface to ensure proper usage of the system.
 */
public class HashStoreFactory {
    private static final Log logHashStore = LogFactory.getLog(HashStoreFactory.class);
    // Get properties from configuration file and refactor
    private final FileHashStore filehs;
    private final int depth = 3;
    private final int width = 2;
    private final String algorithm = "SHA-256";

    /**
     * Default constructor for HashStoreFactory
     * 
     * @param storeDirectory Full file path (ex. /usr/org/metacat/objects)
     * @throws IllegalArgumentException Depth, width must be greater than 0
     * @throws IOException              Issue when creating storeDirectory
     */
    public HashStoreFactory(Path storeDirectory)
            throws IllegalArgumentException, IOException {
        // TODO: Revise constructor
        try {
            this.filehs = new FileHashStore(this.depth, this.width, this.algorithm, storeDirectory);
        } catch (IllegalArgumentException iae) {
            logHashStore
                    .error("Unable to initialize FileHashStore - storeDirectory supplied: " + storeDirectory.toString()
                            + ". Illegal Argument Exception: " + iae.getMessage());
            throw iae;
        }
    }

    /**
     * TODO: Revise factory method
     * 
     * @return
     */
    public FileHashStore createHashStore() {
        return this.filehs;
    }

}

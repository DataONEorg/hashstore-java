package org.dataone.hashstore.hashfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * HashFileStore handles IO operations for HashStore
 */
public class HashFileStore {
    private int directoryDepth;
    private int directoryWidth;
    private String algorithm;
    private String rootDirectory;

    /**
     * Constructor to initialize HashStore fields and object store directory
     * 
     * @param depth
     * @param width
     * @param algorithm
     * @param storeDirectory Full file path (ex. /usr/org/objects)
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public HashFileStore(int depth, int width, String algorithm, String storeDirectory)
            throws IllegalArgumentException, IOException {
        // Validate input parameters
        if (depth <= 0 || width <= 0) {
            throw new IllegalArgumentException("Depth and width must be greater than 0.");
        }
        if (algorithm == null || algorithm.isEmpty()) {
            throw new IllegalArgumentException("Algorithm cannot be null or empty.");
        }
        // TODO: Handle when algorithm is not "sha256"

        this.directoryDepth = depth;
        this.directoryWidth = width;
        this.algorithm = algorithm;

        // If no path provided, create default path with user.dir root + /HashFileStore
        Path objectStoreDirectory;
        if (storeDirectory == null) {
            this.rootDirectory = System.getProperty("user.dir");
            String objectPath = "HashFileStore";
            objectStoreDirectory = Paths.get(rootDirectory).resolve(objectPath);
        } else {
            objectStoreDirectory = Paths.get(storeDirectory);
        }

        // Create store directory
        try {
            Files.createDirectories(objectStoreDirectory);
        } catch (IOException e) {
            // TODO: Log IO exeption failure, e
            throw e;
        }
    }

    protected void put() {
        // TODO: Return HashAddress
        return;
    }
}
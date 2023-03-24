package org.dataone.hashstore.hashfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class HashFileStore {
    private int directoryDepth;
    private int directoryWidth;
    private String algorithm;
    private String rootDirectory;

    public HashFileStore(int depth, int width, String algorithm, String storeDirectory)
            throws IllegalArgumentException, IOException {
        // Validate input parameters
        if (depth <= 0 || width <= 0) {
            throw new IllegalArgumentException("Depth and width must be greater than 0.");
        }
        if (algorithm == null || algorithm.isEmpty()) {
            throw new IllegalArgumentException("Algorithm cannot be null or empty.");
        }

        this.directoryDepth = depth;
        this.directoryWidth = width;
        this.algorithm = algorithm;
        this.rootDirectory = System.getProperty("user.dir");
        Path objectStoreDirectory = Paths.get(rootDirectory).resolve(storeDirectory);

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
package org.dataone.hashstore.hashfs;

// import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
    private Path objectStoreDirectory;
    private Path tmpFileDirectory;

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
        if (storeDirectory == null) {
            String rootDirectory = System.getProperty("user.dir");
            String objectPath = "HashFileStore";
            this.objectStoreDirectory = Paths.get(rootDirectory).resolve(objectPath);
        } else {
            this.objectStoreDirectory = Paths.get(storeDirectory);
        }
        this.tmpFileDirectory = this.objectStoreDirectory.resolve("tmp");

        // Create store and tmp directory
        try {
            Files.createDirectories(this.objectStoreDirectory);
            Files.createDirectories(this.tmpFileDirectory);
        } catch (IOException e) {
            // TODO: Log IO exeption failure, e
            throw e;
        }
    }

    protected void put(InputStream object, String algorithm, String checksum) throws IOException {
        // TODO: Return HashAddress
        return;
    }
}
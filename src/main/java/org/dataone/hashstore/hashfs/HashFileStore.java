package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

/**
 * HashFileStore handles IO operations for HashStore
 */
public class HashFileStore {
    private int directoryDepth;
    private int directoryWidth;
    private String objectStoreAlgorithm;
    private Path objectStoreDirectory;
    private Path tmpFileDirectory;
    private HashUtil hsil;
    private String[] supportedHashAlgorithms = { "MD2", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512", "SHA-512/224",
            "SHA-512/256" };

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

        this.directoryDepth = depth;
        this.directoryWidth = width;
        this.objectStoreAlgorithm = algorithm;

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
            this.hsil = new HashUtil();
        } catch (IOException e) {
            // TODO: Log IO exeption failure, e
            throw e;
        }
    }

    /**
     * Takes a given input stream and writes it to its permanent address on disk
     * based on its SHA-256 hex digest value.
     * 
     * Returns a HashAddress object that contains the file id, relative path,
     * absolute path, duplicate status and a checksum map based on the default
     * algorithm list.
     * 
     * @param object
     * @param additionalAlgorithm
     * @param checksum
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public HashAddress put(InputStream object, String additionalAlgorithm, String checksum)
            throws IOException, NoSuchAlgorithmException {
        // Cannot generate additional algorithm if it is not supported
        if (!Arrays.asList(supportedHashAlgorithms).contains(additionalAlgorithm) && additionalAlgorithm != null) {
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " + supportedHashAlgorithms);
        }

        // Generate tmp file and write to it
        File tmpFile = this.hsil.generateTmpFile("tmp", this.tmpFileDirectory.toFile());
        Map<String, String> hexDigests = this.hsil.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if algorithm and checksum is passed
        if (additionalAlgorithm != null && checksum != null) {
            String digestFromHexDigests = hexDigests.get(objectStoreAlgorithm);
            if (checksum != digestFromHexDigests) {
                tmpFile.delete();
                throw new IllegalArgumentException(
                        "Checksum passed does not equal to the calculated hex digest: " + digestFromHexDigests);
            }

        }

        // Gather HashAddress elements and prepare object permanent address
        String objHexDigest = hexDigests.get(this.objectStoreAlgorithm);
        String objRelativePath = this.hsil.shard(directoryDepth, directoryWidth, objHexDigest);
        String objAbsolutePath = this.objectStoreDirectory.toString() + objRelativePath;
        File objHashAddress = new File(objAbsolutePath);

        // Move object
        boolean isDuplicate = this.hsil.move(tmpFile, objHashAddress);
        if (isDuplicate) {
            tmpFile.delete();
            objHexDigest = null;
            objRelativePath = null;
            objAbsolutePath = null;
            hexDigests = null;
        }

        HashAddress hashAddress = new HashAddress(objHexDigest, objRelativePath, objAbsolutePath, isDuplicate,
                hexDigests);
        return hashAddress;
    }
}
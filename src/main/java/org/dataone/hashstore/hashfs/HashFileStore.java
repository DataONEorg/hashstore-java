package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
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
    private HashUtil hsil = new HashUtil();

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
        boolean algorithmSupported = this.hsil.validateAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " +
                            this.hsil.supportedHashAlgorithms);
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
        } catch (IOException e) {
            // TODO: Log IO exeption failure, e
            throw e;
        }
    }

    /**
     * Stores a file to the Hash File Store
     * 
     * @param object
     * @param abId
     * @param additionalAlgorithm
     * @param checksum
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     */
    public HashAddress putObject(InputStream object, String abId, String additionalAlgorithm, String checksum)
            throws IOException, NoSuchAlgorithmException {
        HashAddress hashad = this.put(object, abId, additionalAlgorithm, checksum);
        return hashad;
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
    protected HashAddress put(InputStream object, String abId, String additionalAlgorithm, String checksum)
            throws IOException, NoSuchAlgorithmException {
        // Cannot generate additional algorithm if it is not supported
        boolean algorithmSupported = this.hsil.validateAlgorithm(additionalAlgorithm);
        if (!algorithmSupported) {
            // TODO: Log failure - include signature values
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " + this.hsil.supportedHashAlgorithms);
        }

        // Generate tmp file and write to it
        File tmpFile = this.hsil.generateTmpFile("tmp", this.tmpFileDirectory.toFile());
        Map<String, String> hexDigests = this.hsil.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if algorithm and checksum is passed
        if (additionalAlgorithm != null && checksum != null) {
            String digestFromHexDigests = hexDigests.get(this.objectStoreAlgorithm);
            if (checksum != digestFromHexDigests) {
                tmpFile.delete();
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum supplied does not equal to the calculated hex digest: " + digestFromHexDigests
                                + ". Deleting tmpFile: " + tmpFile.toString());
            }

        }

        // Gather HashAddress elements and prepare object permanent address
        String objAuthorityId = this.hsil.getHexDigest(abId, this.objectStoreAlgorithm);
        String objRelativePath = this.hsil.shard(directoryDepth, directoryWidth, objAuthorityId);
        String objAbsolutePath = this.objectStoreDirectory.toString() + objRelativePath;
        File objHashAddress = new File(objAbsolutePath);

        // Move object
        boolean isDuplicate = this.hsil.move(tmpFile, objHashAddress);
        if (isDuplicate) {
            tmpFile.delete();
            objAuthorityId = null;
            objRelativePath = null;
            objAbsolutePath = null;
            hexDigests = null;
        }

        HashAddress hashAddress = new HashAddress(objAuthorityId, objRelativePath, objAbsolutePath, isDuplicate,
                hexDigests);
        return hashAddress;
    }

}
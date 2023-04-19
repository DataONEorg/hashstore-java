package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
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
     * Constructor to initialize HashStore fields and object store directory. If
     * storeDirectory is null or an empty string, a default path will be generated
     * based on the user's root folder + "/HashFileStore".
     * 
     * Two directories will be created based on the given storeDirectory string:
     * - .../objects
     * - .../objects/tmp
     * 
     * @param depth
     * @param width
     * @param algorithm
     * @param storeDirectory Full file path (ex. /usr/org/)
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
        if (storeDirectory == null || storeDirectory == "") {
            String rootDirectory = System.getProperty("user.dir");
            String defaultPath = "HashFileStore";
            this.objectStoreDirectory = Paths.get(rootDirectory).resolve(defaultPath).resolve("objects");
        } else {
            this.objectStoreDirectory = Paths.get(storeDirectory).resolve("objects");
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
     * Stores a file to the Hash File Store.
     * 
     * @param object
     * @param abId
     * @param additionalAlgorithm
     * @param checksum
     * @param checksumAlgorithm
     * 
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws SecurityException
     */
    public HashAddress putObject(InputStream object, String abId, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException {
        HashAddress hashad = this.put(object, abId, additionalAlgorithm, checksum, checksumAlgorithm);
        return hashad;
    }

    /**
     * Takes a given input stream and writes it to its permanent address on disk
     * based on the SHA-256 hex digest value of an authority based identifier,
     * which is usually a persistent identifier (pid).
     * 
     * If an additional algorithm is provided and supported, its respective hex
     * digest value will be included in hexDigests map. If a checksum and
     * checksumAlgorithm is provided, HashFileStore will validate the given
     * checksum against the hex digest produced of the supplied checksumAlgorithm.
     * 
     * Returns a HashAddress object that contains the file id, relative path,
     * absolute path, duplicate status and a checksum map based on the default
     * algorithm list.
     * 
     * @param object
     * @param abId                authority based identifier
     * @param additionalAlgorithm optional checksum value to generate in hex digests
     * @param checksum
     * @param checksumAlgorithm
     * 
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws SecurityException
     * @throws FileNotFoundException
     */
    protected HashAddress put(InputStream object, String abId, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException {
        // Cannot generate additional algorithm if it is not supported
        boolean algorithmSupported = this.hsil.validateAlgorithm(additionalAlgorithm);
        boolean checksumAlgorithmSupported = this.hsil.validateAlgorithm(checksumAlgorithm);
        if (!algorithmSupported) {
            // TODO: Log failure - include signature values
            throw new IllegalArgumentException(
                    "Additional algorithm not supported - unable to generate additional hex digest value. Supported algorithms: "
                            + this.hsil.supportedHashAlgorithms);
        }
        if (!checksumAlgorithmSupported) {
            // TODO: Log failure - include signature values
            throw new IllegalArgumentException(
                    "Checksum algorithm not supported - cannot be used to validate object. Supported algorithms: "
                            + this.hsil.supportedHashAlgorithms);
        }

        // Generate tmp file and write to it
        File tmpFile = this.hsil.generateTmpFile("tmp", this.tmpFileDirectory.toFile());
        Map<String, String> hexDigests = this.hsil.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if checksum and checksum algorithm is passed
        if (additionalAlgorithm != null && checksum != null) {
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (checksum != digestFromHexDigests) {
                tmpFile.delete();
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum supplied does not equal to the calculated hex digest: " + digestFromHexDigests
                                + "Checksum provided: " + checksum + ". Deleting tmpFile: " + tmpFile.toString());
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
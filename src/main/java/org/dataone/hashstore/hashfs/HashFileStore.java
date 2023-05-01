package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
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
    private final int directoryDepth;
    private final int directoryWidth;
    private final String objectStoreAlgorithm;
    private final Path objectStoreDirectory;
    private final Path tmpFileDirectory;
    private HashUtil hashUtil = new HashUtil();

    /**
     * Constructor to initialize HashStore fields and object store directory. If
     * storeDirectory is null or an empty string, a default path will be generated
     * based on the user's root folder + "/HashFileStore".
     * 
     * Two directories will be created based on the given storeDirectory string:
     * - .../objects
     * - .../objects/tmp
     * 
     * @param depth          Number of directories created from a given hex digest
     * @param width          Width of the directories
     * @param algorithm      Algorithm used for the permanent address
     * @param storeDirectory Desired absolute file path (ex. /usr/org/)
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public HashFileStore(int depth, int width, String algorithm, Path storeDirectory)
            throws IllegalArgumentException, IOException {
        // Validate input parameters
        if (depth <= 0 || width <= 0) {
            throw new IllegalArgumentException("Depth and width must be greater than 0.");
        }
        if (algorithm == null || algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException("Algorithm cannot be null or empty.");
        }
        boolean algorithmSupported = this.hashUtil.isValidAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " +
                            Arrays.toString(this.hashUtil.supportedHashAlgorithms));
        }

        // If no path provided, create default path with user.dir root + /HashFileStore
        if (storeDirectory == null) {
            String rootDirectory = System.getProperty("user.dir");
            String defaultPath = "HashFileStore";
            this.objectStoreDirectory = Paths.get(rootDirectory).resolve(defaultPath).resolve("objects");
        } else {
            this.objectStoreDirectory = storeDirectory.resolve("objects");
        }
        // Resolve tmp object directory path
        this.tmpFileDirectory = this.objectStoreDirectory.resolve("tmp");
        // Physically create store and tmp directory
        try {
            Files.createDirectories(this.objectStoreDirectory);
            Files.createDirectories(this.tmpFileDirectory);
        } catch (IOException e) {
            // TODO: Log IO exeption failure, e
            throw e;
        }
        // Finalize instance variables
        this.directoryDepth = depth;
        this.directoryWidth = width;
        this.objectStoreAlgorithm = algorithm;
    }

    /**
     * Put an object from a given InputStream to this.objectStoreDirectory
     * Encapsulates this.put()
     * 
     * @param object
     * @param pid                 authority based identifier
     * @param additionalAlgorithm optional checksum value to generate in hex digests
     * @param checksum            value of checksum to validate against
     * @param checksumAlgorithm   algorithm of checksum submitted
     * 
     * @return A HashAddress object that contains the file id, relative path,
     *         absolute path, duplicate status and a checksum map based on the
     *         default algorithm list.
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws SecurityException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    public HashAddress putObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException {
        HashAddress hashAddress = this.put(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
        return hashAddress;
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
     * @param object
     * @param pid                 authority based identifier
     * @param additionalAlgorithm optional checksum value to generate in hex digests
     * @param checksum            value of checksum to validate against
     * @param checksumAlgorithm   algorithm of checksum submitted
     * 
     * @return A HashAddress object that contains the file id, relative path,
     *         absolute path, duplicate status and a checksum map based on the
     *         default algorithm list.
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws SecurityException
     * @throws FileNotFoundException
     * @throws FileAlreadyExistsException
     * @throws IllegalArgumentException
     * @throws NullPointerException
     */
    protected HashAddress put(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException {
        if (object == null) {
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        // pid cannot be empty or null
        if (pid == null || pid.trim().isEmpty()) {
            // TODO: Log failure - include signature values
            throw new IllegalArgumentException("The pid cannot be null or empty");
        }

        // Checksum cannot be empty or null if checksumAlgorithm is passed
        if (checksumAlgorithm != null & checksum != null) {
            if (checksum.trim().isEmpty()) {
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum cannot be null or empty when a checksumAlgorithm is supplied.");
            }
        }
        // Cannot generate additional or checksum algorithm if it is not supported
        if (additionalAlgorithm != null) {
            boolean algorithmSupported = this.hashUtil.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Additional algorithm not supported - unable to generate additional hex digest value. additionalAlgorithm: "
                                + additionalAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(this.hashUtil.supportedHashAlgorithms));
            }
        }
        if (checksumAlgorithm != null) {
            boolean checksumAlgorithmSupported = this.hashUtil.isValidAlgorithm(checksumAlgorithm);
            if (!checksumAlgorithmSupported) {
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum algorithm not supported - cannot be used to validate object. checksumAlgorithm: "
                                + checksumAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(this.hashUtil.supportedHashAlgorithms));
            }
        }

        // Gather HashAddress elements and prepare object permanent address
        String objAuthorityId = this.hashUtil.getHexDigest(pid, this.objectStoreAlgorithm);
        String objShardString = this.hashUtil.shard(this.directoryDepth, this.directoryWidth, objAuthorityId);
        String objAbsolutePathString = this.objectStoreDirectory.toString() + "/" + objShardString;
        File objHashAddress = new File(objAbsolutePathString);
        // If file (pid hash) exists, reject request immediately
        if (objHashAddress.exists()) {
            throw new FileAlreadyExistsException("File already exists for pid: " + pid);
        }

        // Generate tmp file and write to it
        File tmpFile = this.hashUtil.generateTmpFile("tmp", this.tmpFileDirectory.toFile());
        Map<String, String> hexDigests = this.hashUtil.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if checksum and checksum algorithm is passed
        if (checksumAlgorithm != null && checksum != null) {
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (!checksum.equals(digestFromHexDigests)) {
                tmpFile.delete();
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum supplied does not equal to the calculated hex digest: " + digestFromHexDigests
                                + ". Checksum provided: " + checksum + ". Deleting tmpFile: " + tmpFile.toString());
            }
        }

        // Move object
        boolean isNotDuplicate = this.hashUtil.move(tmpFile, objHashAddress);
        if (!isNotDuplicate) {
            tmpFile.delete();
            objAuthorityId = null;
            objShardString = null;
            objAbsolutePathString = null;
        }

        // Create HashAddress object to return with pertinent data
        HashAddress hashAddress = new HashAddress(objAuthorityId, objShardString, objAbsolutePathString, isNotDuplicate,
                hexDigests);
        return hashAddress;
    }

}
package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * HashFileStore is a class that manages storage of objects to disk using
 * SHA-256 hex digest of an authority-based identifier as a key (usually in the
 * form of a pid). It also provides an interface for interacting with stored
 * objects and metadata.
 *
 */
public class HashFileStore {
    private static final Log logHashFileStore = LogFactory.getLog(HashFileStore.class);
    private final int directoryDepth;
    private final int directoryWidth;
    private final String objectStoreAlgorithm;
    private final Path objectStoreDirectory;
    private final Path tmpFileDirectory;
    public static final String[] SUPPORTED_HASH_ALGORITHMS = { "MD2", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512",
            "SHA-512/224", "SHA-512/256" };
    public static final String[] DEFAULT_HASH_ALGORITHMS = { "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512" };

    /**
     * Constructor to initialize HashStore fields and object store directory. If
     * storeDirectory is null or an empty string, a default path will be generated
     * based on the user's working directory + "/HashFileStore".
     * 
     * Two directories will be created based on the given storeDirectory string:
     * - .../objects
     * - .../objects/tmp
     * 
     * @param depth          Number of directories created from a given hex digest
     * @param width          Width of the directories
     * @param algorithm      Algorithm used for the permanent address
     * @param storeDirectory Desired absolute file path (ex. /usr/org/)
     * @throws IllegalArgumentException Constructor arguments cannot be null, empty
     *                                  or less than 0
     * @throws IOException              Issue with creating directories
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
        boolean algorithmSupported = this.isValidAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " +
                            Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
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
        } catch (IOException ioe) {
            logHashFileStore.error("Failed to initialize HashFileStore - unable to create directories. Exception: "
                    + ioe.getMessage());
            throw ioe;
        }
        // Finalize instance variables
        this.directoryDepth = depth;
        this.directoryWidth = width;
        this.objectStoreAlgorithm = algorithm;
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
     * @param object              inputstream for file
     * @param pid                 authority based identifier
     * @param additionalAlgorithm optional checksum value to generate in hex digests
     * @param checksum            value of checksum to validate against
     * @param checksumAlgorithm   algorithm of checksum submitted
     * 
     * @return A HashAddress object that contains the file id, relative path,
     *         absolute path, duplicate status and a checksum map based on the
     *         default algorithm list.
     * @throws IOException                     I/O Error when writing file,
     *                                         generating checksums, moving file or
     *                                         deleting tmpFile upon duplicate found
     * @throws NoSuchAlgorithmException        When additionalAlgorithm or
     *                                         checksumAlgorithm is invalid or not
     *                                         found
     * @throws SecurityException               Insufficient permissions to
     *                                         read/access files or when
     *                                         generating/writing to a file
     * @throws FileNotFoundException           tmpFile not found during store
     * @throws FileAlreadyExistsException      Duplicate object in store exists
     * @throws IllegalArgumentException        When signature values are empty
     *                                         (checksum, pid, etc.)
     * @throws NullPointerException            Arguments are null for pid or object
     * @throws AtomicMoveNotSupportedException When attempting to move files across
     *                                         file systems
     */
    public HashAddress putObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException,
            AtomicMoveNotSupportedException {
        logHashFileStore.info("HashFileStore.putObject - Called to put object for pid: " + pid);
        // Begin input validation
        if (object == null) {
            logHashFileStore.error("HashFileStore.putObject - InputStream cannot be null, pid: " + pid);
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        // pid cannot be empty or null
        if (pid == null || pid.trim().isEmpty()) {
            logHashFileStore.error("HashFileStore.putObject - pid cannot be null or empty. pid: " + pid);
            throw new IllegalArgumentException("The pid cannot be null or empty");
        }
        // If validation is desired, checksumAlgorithm and checksum must both be present
        // and additionalChecksum must be equal to checksumAlgorithm if the
        // checksumAlgorithm is not included in the default hex digest map
        this.validateChecksumParameters(checksum, checksumAlgorithm, additionalAlgorithm);

        // Gather HashAddress elements and prepare object permanent address
        String objAuthorityId = this.getHexDigest(pid, this.objectStoreAlgorithm);
        String objShardString = this.getHierarchicalPathString(this.directoryDepth, this.directoryWidth,
                objAuthorityId);
        String objAbsolutePathString = this.objectStoreDirectory.toString() + "/" + objShardString;
        File objHashAddress = new File(objAbsolutePathString);
        // If file (pid hash) exists, reject request immediately
        if (objHashAddress.exists()) {
            logHashFileStore.error("HashFileStore.putObject - File already exists for pid: " + pid
                    + ". Object address: " + objAbsolutePathString);
            throw new FileAlreadyExistsException("File already exists for pid: " + pid);
        }

        // Generate tmp file and write to it
        logHashFileStore.debug("HashFileStore.putObject - Generating tmpFile");
        File tmpFile = this.generateTmpFile("tmp", this.tmpFileDirectory);
        Map<String, String> hexDigests = this.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if checksum and checksum algorithm is passed
        if (checksumAlgorithm != null && checksum != null) {
            logHashFileStore.info("HashFileStore.putObject - Validating object");
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (digestFromHexDigests == null) {
                logHashFileStore.error(
                        "HashFileStore.putObject - checksum not found in hex digest map when validating object. checksumAlgorithm checked: "
                                + checksumAlgorithm);
                throw new NoSuchAlgorithmException(
                        "HashFileStore.putObject - checksum not found in hex digest map when validating object, checksumAlgorithm checked: "
                                + checksumAlgorithm);
            }
            if (!checksum.equals(digestFromHexDigests)) {
                // Delete tmp File
                boolean deleteStatus = tmpFile.delete();
                if (!deleteStatus) {
                    throw new IOException("Object cannot be validated and failed to delete tmpFile: " + tmpFile);
                }
                logHashFileStore.error(
                        "HashFileStore.putObject - Checksum supplied does not equal to the calculated hex digest: "
                                + digestFromHexDigests + ". Checksum provided: " + checksum + ". Deleting tmpFile: "
                                + tmpFile);
                throw new IllegalArgumentException(
                        "Checksum supplied does not equal to the calculated hex digest: " + digestFromHexDigests
                                + ". Checksum provided: " + checksum + ". Deleting tmpFile: " + tmpFile);
            }
        }

        // Move object
        boolean isDuplicate = true;
        logHashFileStore.debug("HashFileStore.putObject - Moving object: " + tmpFile.toString() + ". Destination: "
                + objAbsolutePathString);
        if (objHashAddress.exists()) {
            boolean deleteStatus = tmpFile.delete();
            if (!deleteStatus) {
                throw new IOException("Object is a duplicate. Attempted to delete tmpFile but failed: " + tmpFile);
            }
            objAuthorityId = null;
            objShardString = null;
            objAbsolutePathString = null;
            logHashFileStore.info(
                    "HashFileStore.putObject - Did not move object, duplicate file found for pid: " + pid);
        } else {
            boolean hasMoved = this.move(tmpFile, objHashAddress);
            if (hasMoved) {
                isDuplicate = false;
            }
            logHashFileStore
                    .info("HashFileStore.putObject - Move object success, permanent address: " + objAbsolutePathString);
        }

        // Create HashAddress object to return with pertinent data
        HashAddress hashAddress = new HashAddress(objAuthorityId, objShardString, objAbsolutePathString, isDuplicate,
                hexDigests);
        return hashAddress;
    }

    public void validateChecksumParameters(String checksum, String checksumAlgorithm, String additionalAlgorithm) {
        if (checksum != null && !checksum.trim().isEmpty()) {
            if (checksumAlgorithm == null) {
                // ChecksumAlgorithm cannot be null if checksum is supplied
                logHashFileStore.error(
                        "HashFileStore.validateChecksumParameters - checksumAlgorithm cannot be null if checksum is supplied");
                throw new NullPointerException(
                        "checksumAlgorithm cannot be null if checksum is supplied, checksum and checksumAlgorithm must both be null if validation is not desired.");
            } else if (checksumAlgorithm.trim().isEmpty()) {
                // ChecksumAlgorithm cannot be empty when checksum supplied
                logHashFileStore.error(
                        "HashFileStore.validateChecksumParameters - checksumAlgorithm cannot be empty if checksum is supplied");
                throw new IllegalArgumentException(
                        "checksumAlgorithm cannot be empty when checksum supplied, checksum and checksumAlgorithm must both be null if unused.");
            } else if (!this.isValidAlgorithm(checksumAlgorithm)) {
                // ChecksumAlgorithm not supported
                logHashFileStore.error(
                        "HashFileStore.validateChecksumParameters - checksumAlgorithm not supported, checksumAlgorithm: "
                                + checksumAlgorithm);
                throw new IllegalArgumentException(
                        "Checksum algorithm not supported - cannot be used to validate object. checksumAlgorithm: "
                                + checksumAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
            }
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            // checksum cannot be null or empty if checksumAlgorithm is supplied
            if (checksum == null) {
                logHashFileStore.error(
                        "HashFileStore.validateChecksumParameters - checksum cannot be null if checksumAlgorithm supplied.");
                throw new NullPointerException(
                        "Checksum cannot be null if checksumAlgorithm is supplied, checksum and checksumAlgorithm must both be null if unused.");
            } else if (checksum.trim().isEmpty()) {
                // ChecksumAlgorithm cannot be empty when checksum supplied
                logHashFileStore.error(
                        "HashFileStore.validateChecksumParameters - checksum cannot be empty if checksumAlgorithm is supplied");
                throw new IllegalArgumentException(
                        "checksum cannot be empty when checksumAlgorithm supplied, checksum and checksumAlgorithm must both be null if unused.");
            }
        }
        if (checksumAlgorithm != null || checksum != null) {
            if (!checksumAlgorithm.trim().isEmpty() || !checksum.trim().isEmpty()) {
                boolean includedAlgo = this.isDefaultAlgorithm(checksumAlgorithm);
                if (!includedAlgo) {
                    if (!checksumAlgorithm.equals(additionalAlgorithm)) {
                        logHashFileStore.error(
                                "HashFileStore.putObject - checksumAlgorithm is supported but not included in the default list. additionalAlgorithm must be equal to checksumAlgorithm in order to proceed with object validation. additionalAlgorithm: "
                                        + additionalAlgorithm + ". checksumAlgorithm: " + checksumAlgorithm);
                        throw new IllegalArgumentException(
                                "checksumAlgorithm is supported but not included in the default list. additionalAlgorithm must be equal to checksumAlgorithm in order to proceed with object validation. additionalAlgorithm: "
                                        + additionalAlgorithm + ". checksumAlgorithm: " + checksumAlgorithm);
                    }
                }
            }
        }
    }

    /**
     * Checks whether a given algorithm is supported based on the HashUtil class
     * variable supportedHashAlgorithms
     * 
     * @param algorithm string value (ex. SHA-256)
     * @return true if an algorithm is supported
     * @throws NullPointerException     algorithm cannot be null
     * @throws IllegalArgumentException algorithm cannot be empty
     */
    public boolean isValidAlgorithm(String algorithm) throws NullPointerException {
        if (algorithm == null) {
            throw new NullPointerException("Algorithm value supplied is null");
        }
        if (algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException("Algorithm value supplied is empty");
        }
        boolean isValid = Arrays.asList(SUPPORTED_HASH_ALGORITHMS).contains(algorithm);
        return isValid;
    }

    /**
     * Checks whether a given algorithm is included in the default algorithm list
     * 
     * @param algorithm string value (ex. SHA-256)
     * @return true if an algorithm is part of the default list
     * @throws NullPointerException     algorithm cannot be null
     * @throws IllegalArgumentException algorithm cannot be empty
     */
    public boolean isDefaultAlgorithm(String algorithm) throws NullPointerException {
        if (algorithm == null) {
            throw new NullPointerException("Algorithm value supplied is null");
        }
        if (algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException("Algorithm value supplied is empty");
        }
        boolean isDefault = Arrays.asList(DEFAULT_HASH_ALGORITHMS).contains(algorithm);
        return isDefault;
    }

    /**
     * Given a string and supported algorithm returns the hex digest
     * 
     * @param pid       authority based identifier or persistent identifier
     * @param algorithm string value (ex. SHA-256)
     * 
     * @return Hex digest of the given string in lower-case
     * @throws IllegalArgumentException String or algorithm cannot be null or empty
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    protected String getHexDigest(String pid, String algorithm)
            throws NoSuchAlgorithmException, IllegalArgumentException {
        if (algorithm == null || algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Algorithm cannot be null or empty");
        }
        if (pid == null || pid.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "String cannot be null or empty");
        }
        boolean algorithmSupported = this.isValidAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new NoSuchAlgorithmException(
                    "Algorithm not supported. Supported algorithms: " + Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
        }
        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = pid.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        String stringDigest = DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
        return stringDigest;
    }

    /**
     * Generates a hierarchical path by dividing a given digest into tokens
     * of fixed width, and concatenating them with '/' as the delimiter.
     *
     * @param dirDepth integer to represent number of directories
     * @param dirWidth width of each directory
     * @param digest   value to shard
     * @return String
     */
    protected String getHierarchicalPathString(int dirDepth, int dirWidth, String digest) {
        List<String> tokens = new ArrayList<>();
        int digestLength = digest.length();
        for (int i = 0; i < dirDepth; i++) {
            int start = i * dirWidth;
            int end = Math.min((i + 1) * dirWidth, digestLength);
            tokens.add(digest.substring(start, end));
        }
        if (dirDepth * dirWidth < digestLength) {
            tokens.add(digest.substring(dirDepth * dirWidth));
        }
        List<String> stringArray = new ArrayList<>();
        for (String str : tokens) {
            if (!str.trim().isEmpty()) {
                stringArray.add(str);
            }
        }
        String stringShard = String.join("/", stringArray);
        return stringShard;
    }

    /**
     * Creates an empty file in a given location
     * 
     * @param prefix    string to prepend before tmp file
     * @param directory location to create tmp file
     * 
     * @return Temporary file (File) ready to write into
     * @throws IOException       Issues with generating tmpFile
     * @throws SecurityException Insufficient permissions to create tmpFile
     */
    protected File generateTmpFile(String prefix, Path directory) throws IOException, SecurityException {
        Random rand = new Random();
        int randomNumber = rand.nextInt(1000000);
        String newPrefix = prefix + "-" + System.currentTimeMillis() + randomNumber;
        try {
            Path newPath = Files.createTempFile(directory, newPrefix, null);
            File newFile = newPath.toFile();
            logHashFileStore.debug("HashFileStore.generateTmpFile - tmpFile generated: " + newFile.getAbsolutePath());
            return newFile;
        } catch (IOException ioe) {
            logHashFileStore.error("HashFileStore.generateTmpFile - Unable to generate tmpFile: " + ioe.getMessage());
            throw new IOException("Unable to generate tmpFile. IOException: " + ioe.getMessage());
        } catch (SecurityException se) {
            logHashFileStore.error("HashFileStore.generateTmpFile - Unable to generate tmpFile: " + se.getMessage());
            throw new SecurityException("File not allowed (security manager exists): " + se.getMessage());
        }
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap
     * consisting of algorithms and their respective hex digests. If an additional
     * algorithm is supplied and supported, it and its checksum value will be
     * included in the hex digests map.
     * 
     * Default algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
     * 
     * @param tmpFile             file to write input stream data into
     * @param dataStream          input stream of data to store
     * @param additionalAlgorithm additional algorithm to include in hex digest map
     * 
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException Unable to generate new instance of supplied
     *                                  algorithm
     * @throws IOException              Issue with writing file from InputStream
     * @throws SecurityException        Unable to write to tmpFile
     * @throws FileNotFoundException    tmnpFile cannot be found
     */
    protected Map<String, String> writeToTmpFileAndGenerateChecksums(File tmpFile, InputStream dataStream,
            String additionalAlgorithm)
            throws NoSuchAlgorithmException, IOException, FileNotFoundException, SecurityException {
        if (additionalAlgorithm != null) {
            if (additionalAlgorithm.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Additional algorithm cannot be empty");
            }
            boolean algorithmSupported = this.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                throw new IllegalArgumentException(
                        "Algorithm not supported. Supported algorithms: " + Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
            }
        }

        MessageDigest extraAlgo = null;
        Map<String, String> hexDigests = new HashMap<>();

        FileOutputStream os = new FileOutputStream(tmpFile);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        MessageDigest sha384 = MessageDigest.getInstance("SHA-384");
        MessageDigest sha512 = MessageDigest.getInstance("SHA-512");
        if (additionalAlgorithm != null) {
            logHashFileStore.info(
                    "HashFileStore.writeToTmpFileAndGenerateChecksums - Adding additional algorithm to hex digest map, algorithm: "
                            + additionalAlgorithm);
            extraAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }

        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
                md5.update(buffer, 0, bytesRead);
                sha1.update(buffer, 0, bytesRead);
                sha256.update(buffer, 0, bytesRead);
                sha384.update(buffer, 0, bytesRead);
                sha512.update(buffer, 0, bytesRead);
                if (additionalAlgorithm != null) {
                    extraAlgo.update(buffer, 0, bytesRead);
                }
            }
        } catch (IOException ioe) {
            logHashFileStore.error(
                    "HashFileStore.writeToTmpFileAndGenerateChecksums - IOException encountered (os.flush/close or write related): "
                            + ioe.getMessage());
            throw ioe;
        } finally {
            os.flush();
            os.close();
        }

        logHashFileStore.info("HashFileStore.writeToTmpFileAndGenerateChecksums - Object has been written to tmpFile");
        String md5Digest = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put("MD5", md5Digest);
        hexDigests.put("SHA-1", sha1Digest);
        hexDigests.put("SHA-256", sha256Digest);
        hexDigests.put("SHA-384", sha384Digest);
        hexDigests.put("SHA-512", sha512Digest);
        if (additionalAlgorithm != null) {
            String extraDigest = DatatypeConverter.printHexBinary(extraAlgo.digest()).toLowerCase();
            hexDigests.put(additionalAlgorithm, extraDigest);
        }

        return hexDigests;
    }

    /**
     * Moves an object from one location to another if the object does not exist
     * 
     * @param source file to move
     * @param target where to move the file
     * 
     * @return true if file has been moved
     * 
     * @throws FileAlreadyExistsException      Target file already exists
     * @throws IOException                     Unable to create parent directory
     * @throws SecurityException               Insufficient permissions to move file
     * @throws AtomicMoveNotSupportedException When ATOMIC_MOVE is not supported
     *                                         (usually encountered when moving
     *                                         across file systems)
     */
    protected boolean move(File source, File target)
            throws IOException, SecurityException, AtomicMoveNotSupportedException, FileAlreadyExistsException {
        if (target.exists()) {
            throw new FileAlreadyExistsException("File already exists for target: " + target);
        }

        File destinationDirectory = new File(target.getParent());
        // Create parent directory if it doesn't exist
        if (!destinationDirectory.exists()) {
            Path destinationDirectoryPath = destinationDirectory.toPath();
            Files.createDirectories(destinationDirectoryPath);
        }

        // Move file
        Path sourceFilePath = source.toPath();
        Path targetFilePath = target.toPath();
        try {
            Files.move(sourceFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
            return true;
        } catch (AtomicMoveNotSupportedException amnse) {
            logHashFileStore.error(
                    "HashFileStore.move - StandardCopyOption.ATOMIC_MOVE failed. AtomicMove is not supported across file systems. Source: "
                            + source + ". Target: " + target);
            throw amnse;
        } catch (IOException ioe) {
            logHashFileStore.error("HashFileStore.move - Unable to move file. Source: " + source
                    + ". Target: " + target);
            throw ioe;
        }
    }
}
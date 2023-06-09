package org.dataone.hashstore.filehashstore;

import java.io.BufferedReader;
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
import org.dataone.hashstore.HashAddress;
import org.dataone.hashstore.HashStore;

/**
 * FileHashStore is a class that manages storage of objects to disk using
 * SHA-256 hex digest of an authority-based identifier as a key (usually in the
 * form of a pid). It also provides an interface for interacting with stored
 * objects and metadata.
 *
 */
public class FileHashStore implements HashStore {
    private static final Log logFileHashStore = LogFactory.getLog(FileHashStore.class);
    private static final ArrayList<String> objectLockedIds = new ArrayList<>(100);
    private final int DIRECTORY_DEPTH;
    private final int DIRECTORY_WIDTH;
    private final String OBJECT_STORE_ALGORITHM;
    private final Path OBJECT_STORE_DIRECTORY;
    private final Path OBJECT_TMP_FILE_DIRECTORY;

    public static final String[] SUPPORTED_HASH_ALGORITHMS = { "MD2", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512",
            "SHA-512/224", "SHA-512/256" };

    enum DefaultHashAlgorithms {
        MD5,
        SHA_1,
        SHA_256,
        SHA_384,
        SHA_512
    }

    enum HashStoreProperties {
        storePath,
        storeDepth,
        storeWidth,
        storeAlgorithm
    }

    /**
     * Constructor to initialize HashStore fields and object store directory. If
     * storeDirectory is null or an empty string, a default path will be generated
     * based on the user's working directory + "/FileHashStore".
     * 
     * Two directories will be created based on the given storeDirectory string:
     * - .../objects
     * - .../objects/tmp
     * 
     * @param hashstoreProperties HashMap of the HashStore required keys:
     *                            (Path) storePath,
     *                            (int) storeDepth,
     *                            (int) storeWidth
     *                            (String) storeAlgorithm
     * @throws IllegalArgumentException Constructor arguments cannot be null, empty
     *                                  or less than 0
     * @throws IOException              Issue with creating directories
     */
    public FileHashStore(HashMap<String, Object> hashstoreProperties)
            throws IllegalArgumentException, IOException, NoSuchAlgorithmException {
        if (hashstoreProperties == null) {
            logFileHashStore.error("FileHashStore - properties cannot be null.");
            throw new IllegalArgumentException("FileHashStore - properties cannot be null.");

        }
        // Get properties
        Path storePath = (Path) hashstoreProperties.get(HashStoreProperties.storePath.name());
        int storeDepth = (int) hashstoreProperties.get(HashStoreProperties.storeDepth.name());
        int storeWidth = (int) hashstoreProperties.get(HashStoreProperties.storeWidth.name());
        String storeAlgorithm = (String) hashstoreProperties.get(HashStoreProperties.storeAlgorithm.name());

        // Validate input parameters
        if (storeDepth <= 0 || storeWidth <= 0) {
            throw new IllegalArgumentException("Depth and width must be greater than 0.");
        }
        // Ensure algorithm supplied is not empty, not null and supported
        this.validateAlgorithm(storeAlgorithm);

        // If no path provided, create default path with user.dir root + /FileHashStore
        if (storePath == null) {
            String rootDirectory = System.getProperty("user.dir");
            String defaultPath = "FileHashStore";
            this.OBJECT_STORE_DIRECTORY = Paths.get(rootDirectory).resolve(defaultPath).resolve("objects");
        } else {
            this.OBJECT_STORE_DIRECTORY = storePath.resolve("objects");
        }
        // Resolve tmp object directory path
        this.OBJECT_TMP_FILE_DIRECTORY = this.OBJECT_STORE_DIRECTORY.resolve("tmp");
        // Physically create store and tmp directory
        try {
            Files.createDirectories(this.OBJECT_STORE_DIRECTORY);
            Files.createDirectories(this.OBJECT_TMP_FILE_DIRECTORY);
        } catch (IOException ioe) {
            logFileHashStore.error("Failed to initialize FileHashStore - unable to create directories. Exception: "
                    + ioe.getMessage());
            throw ioe;
        }
        // Finalize instance variables
        this.DIRECTORY_DEPTH = storeDepth;
        this.DIRECTORY_WIDTH = storeWidth;
        this.OBJECT_STORE_ALGORITHM = storeAlgorithm;
    }

    @Override
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException, RuntimeException,
            AtomicMoveNotSupportedException {
        logFileHashStore.info("FileHashStore.storeObject - Called to store object for pid: " + pid);
        // Begin input validation
        if (object == null) {
            logFileHashStore.error("FileHashStore.storeObject - InputStream cannot be null, pid: " + pid);
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        if (pid == null || pid.trim().isEmpty()) {
            logFileHashStore.error("FileHashStore.storeObject - pid cannot be null or empty, pid: " + pid);
            throw new IllegalArgumentException("Pid cannot be null or empty, pid: " + pid);
        }
        // Validate algorithms if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null && additionalAlgorithm.trim().isEmpty()) {
            this.validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null && checksumAlgorithm.trim().isEmpty()) {
            this.validateAlgorithm(checksumAlgorithm);
        }

        // Lock pid for thread safety, transaction control and atomic writing
        // A pid can only be stored once and only once, subsequent calls will
        // be accepted but will be rejected if pid hash object exists
        synchronized (objectLockedIds) {
            if (objectLockedIds.contains(pid)) {
                logFileHashStore
                        .warn("FileHashStore.storeObject - Duplicate object request encountered for pid: " + pid);
                throw new RuntimeException(
                        "FileHashStore.storeObject request for pid: " + pid + " already in progress.");
            }
            objectLockedIds.add(pid);
        }

        try {
            logFileHashStore.debug("FileHashStore.storeObject - filehs.putObject request for pid: " + pid
                    + ". additionalAlgorithm: " + additionalAlgorithm + ". checksum: " + checksum
                    + ". checksumAlgorithm: " + checksumAlgorithm);
            // Store object
            HashAddress objInfo = this.putObject(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
            logFileHashStore.info(
                    "FileHashStore.storeObject - Object stored for pid: " + pid + ". Permanent address: "
                            + objInfo.getAbsPath());
            return objInfo;
        } catch (NullPointerException npe) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". NullPointerException: " + npe.getMessage());
            throw npe;
        } catch (IllegalArgumentException iae) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". IllegalArgumentException: " + iae.getMessage());
            throw iae;
        } catch (NoSuchAlgorithmException nsae) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". NoSuchAlgorithmException: " + nsae.getMessage());
            throw nsae;
        } catch (FileAlreadyExistsException faee) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". FileAlreadyExistsException: " + faee.getMessage());
            throw faee;
        } catch (FileNotFoundException fnfe) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". FileNotFoundException: " + fnfe.getMessage());
            throw fnfe;
        } catch (AtomicMoveNotSupportedException amnse) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". AtomicMoveNotSupportedException: " + amnse.getMessage());
            throw amnse;
        } catch (IOException ioe) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". IOException: " + ioe.getMessage());
            throw ioe;
        } catch (SecurityException se) {
            logFileHashStore.error("FileHashStore.storeObject - Cannot store object for pid: " + pid
                    + ". SecurityException: " + se.getMessage());
            throw se;
        } catch (RuntimeException re) {
            logFileHashStore.error("FileHashStore.storeObject - Object was stored for : " + pid
                    + ". But encountered RuntimeException when releasing object lock: " + re.getMessage());
            throw re;
        } finally {
            // Release lock
            synchronized (objectLockedIds) {
                objectLockedIds.remove(pid);
                objectLockedIds.notifyAll();
            }
        }
    }

    @Override
    public String storeSysmeta(InputStream sysmeta, String pid) throws Exception {
        // TODO: Implement method
        return null;
    }

    @Override
    public BufferedReader retrieveObject(String pid) throws Exception {
        // TODO: Implement method
        return null;
    }

    @Override
    public String retrieveSysmeta(String pid) throws Exception {
        // TODO: Implement method
        return null;
    }

    @Override
    public boolean deleteObject(String pid) throws Exception {
        // TODO: Implement method
        return false;
    }

    @Override
    public boolean deleteSysmeta(String pid) throws Exception {
        // TODO: Implement method
        return false;
    }

    @Override
    public String getHexDigest(String pid, String algorithm) throws Exception {
        // TODO: Implement method
        return null;
    }

    /**
     * Takes a given input stream and writes it to its permanent address on disk
     * based on the SHA-256 hex digest value of an authority based identifier,
     * which is usually a persistent identifier (pid).
     * 
     * If an additional algorithm is provided and supported, its respective hex
     * digest value will be included in hexDigests map. If a checksum and
     * checksumAlgorithm is provided, FileHashStore will validate the given
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
    protected HashAddress putObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException,
            AtomicMoveNotSupportedException {
        logFileHashStore.info("FileHashStore.putObject - Called to put object for pid: " + pid);
        // Begin input validation
        if (object == null) {
            logFileHashStore.error("FileHashStore.putObject - InputStream cannot be null, pid: " + pid);
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        // pid cannot be empty or null
        if (pid == null || pid.trim().isEmpty()) {
            logFileHashStore.error("FileHashStore.putObject - pid cannot be null or empty. pid: " + pid);
            throw new IllegalArgumentException("The pid cannot be null or empty");
        }
        // Validate algorithms if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null && !additionalAlgorithm.trim().isEmpty()) {
            this.validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            this.validateAlgorithm(checksumAlgorithm);
        }
        // If validation is desired, checksumAlgorithm and checksum must both be present
        boolean requestValidation = this.verifyChecksumParameters(checksum, checksumAlgorithm);

        // Gather HashAddress elements and prepare object permanent address
        String objAuthorityId = this.getPidHexDigest(pid, this.OBJECT_STORE_ALGORITHM);
        String objShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                objAuthorityId);
        String objAbsolutePathString = this.OBJECT_STORE_DIRECTORY.toString() + "/" + objShardString;
        File objHashAddress = new File(objAbsolutePathString);
        // If file (pid hash) exists, reject request immediately
        if (objHashAddress.exists()) {
            logFileHashStore.error("FileHashStore.putObject - File already exists for pid: " + pid
                    + ". Object address: " + objAbsolutePathString);
            throw new FileAlreadyExistsException("File already exists for pid: " + pid);
        }

        // Generate tmp file and write to it
        logFileHashStore.debug("FileHashStore.putObject - Generating tmpFile");
        File tmpFile = this.generateTmpFile("tmp", this.OBJECT_TMP_FILE_DIRECTORY);
        Map<String, String> hexDigests = this.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm);

        // Validate object if checksum and checksum algorithm is passed
        if (requestValidation) {
            logFileHashStore.info("FileHashStore.putObject - Validating object");
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (digestFromHexDigests == null) {
                logFileHashStore.error(
                        "FileHashStore.putObject - checksum not found in hex digest map when validating object. checksumAlgorithm checked: "
                                + checksumAlgorithm);
                throw new NoSuchAlgorithmException(
                        "FileHashStore.putObject - checksum not found in hex digest map when validating object, checksumAlgorithm checked: "
                                + checksumAlgorithm);
            }
            if (!checksum.equals(digestFromHexDigests)) {
                // Delete tmp File
                boolean deleteStatus = tmpFile.delete();
                if (!deleteStatus) {
                    throw new IOException("Object cannot be validated and failed to delete tmpFile: " + tmpFile);
                }
                logFileHashStore.error(
                        "FileHashStore.putObject - Checksum supplied does not equal to the calculated hex digest: "
                                + digestFromHexDigests + ". Checksum provided: " + checksum + ". Deleting tmpFile: "
                                + tmpFile);
                throw new IllegalArgumentException(
                        "Checksum supplied does not equal to the calculated hex digest: " + digestFromHexDigests
                                + ". Checksum provided: " + checksum + ". Deleting tmpFile: " + tmpFile);
            }
        }

        // Move object
        boolean isDuplicate = true;
        logFileHashStore.debug("FileHashStore.putObject - Moving object: " + tmpFile.toString() + ". Destination: "
                + objAbsolutePathString);
        if (objHashAddress.exists()) {
            boolean deleteStatus = tmpFile.delete();
            if (!deleteStatus) {
                throw new IOException("Object is a duplicate. Attempted to delete tmpFile but failed: " + tmpFile);
            }
            objAuthorityId = null;
            objShardString = null;
            objAbsolutePathString = null;
            logFileHashStore.info(
                    "FileHashStore.putObject - Did not move object, duplicate file found for pid: " + pid);
        } else {
            boolean hasMoved = this.move(tmpFile, objHashAddress);
            if (hasMoved) {
                isDuplicate = false;
            }
            logFileHashStore
                    .info("FileHashStore.putObject - Move object success, permanent address: " + objAbsolutePathString);
        }

        // Create HashAddress object to return with pertinent data
        return new HashAddress(objAuthorityId, objShardString, objAbsolutePathString, isDuplicate,
                hexDigests);
    }

    /**
     * Determines whether an object will be verified with a given checksum and
     * checksumAlgorithm
     * 
     * @param checksum          Value of checksum
     * @param checksumAlgorithm Ex. "SHA-256"
     * @return True if validation is required
     * @throws NoSuchAlgorithmException If checksumAlgorithm supplied is not
     *                                  supported
     */
    protected boolean verifyChecksumParameters(String checksum, String checksumAlgorithm)
            throws NoSuchAlgorithmException {
        // If checksum is supplied, checksumAlgorithm cannot be empty
        if (checksum != null && !checksum.trim().isEmpty()) {
            if (checksumAlgorithm == null) {
                logFileHashStore
                        .error("FileHashStore.verifyChecksumParameters - Validation requested but checksumAlgorithm is null.");
                throw new IllegalArgumentException("Validation requested but checksum is empty.");
            }
            if (checksumAlgorithm.trim().isEmpty()) {
                logFileHashStore
                        .error("FileHashStore.verifyChecksumParameters - Validation requested but checksumAlgorithm is empty.");
                throw new IllegalArgumentException("Validation requested but checksum is empty.");
            }
        }
        // Ensure algorithm is supported, not null and not empty
        boolean requestValidation = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            requestValidation = this.validateAlgorithm(checksumAlgorithm);
            // Ensure checksum is not null or empty if checksumAlgorithm is supplied
            if (requestValidation) {
                if (checksum == null) {
                    logFileHashStore
                            .error("FileHashStore.verifyChecksumParameters - Validation requested but checksum is null.");
                    throw new NullPointerException("Validation requested but checksum is null.");
                }
                if (checksum.trim().isEmpty()) {
                    logFileHashStore
                            .error("FileHashStore.verifyChecksumParameters - Validation requested but checksum is empty.");
                    throw new IllegalArgumentException("Validation requested but checksum is empty.");
                }
            }
        }
        return requestValidation;
    }

    /**
     * Checks whether a given algorithm is supported based on class variable
     * supportedHashAlgorithms
     * 
     * @param algorithm string value (ex. SHA-256)
     * @return True if an algorithm is supported
     * @throws NullPointerException     algorithm cannot be null
     * @throws IllegalArgumentException algorithm cannot be empty
     * @throws NoSuchAlgorithmException algorithm not supported
     */
    protected boolean validateAlgorithm(String algorithm)
            throws NullPointerException, IllegalArgumentException, NoSuchAlgorithmException {
        if (algorithm == null) {
            logFileHashStore.error("FileHashStore.validateAlgorithm - algorithm is null.");
            throw new NullPointerException("Algorithm value supplied is null");
        }
        if (algorithm.trim().isEmpty()) {
            logFileHashStore.error("FileHashStore.validateAlgorithm - algorithm is empty.");
            throw new IllegalArgumentException("Algorithm value supplied is empty");
        }
        boolean algorithmSupported = Arrays.asList(SUPPORTED_HASH_ALGORITHMS).contains(algorithm);
        if (!algorithmSupported) {
            throw new NoSuchAlgorithmException(
                    "Algorithm not supported: " + algorithm + ". Supported algorithms: " +
                            Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
        }
        return true;
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
    protected String getPidHexDigest(String pid, String algorithm)
            throws NoSuchAlgorithmException, IllegalArgumentException {
        if (algorithm == null || algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Algorithm cannot be null or empty");
        }
        if (pid == null || pid.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "String cannot be null or empty");
        }
        boolean algorithmSupported = this.validateAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new NoSuchAlgorithmException(
                    "Algorithm not supported. Supported algorithms: " + Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
        }
        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = pid.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        // stringDigest
        return DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
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
        // stringShard
        return String.join("/", stringArray);
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
            logFileHashStore.debug("FileHashStore.generateTmpFile - tmpFile generated: " + newFile.getAbsolutePath());
            return newFile;
        } catch (IOException ioe) {
            logFileHashStore.error("FileHashStore.generateTmpFile - Unable to generate tmpFile: " + ioe.getMessage());
            throw new IOException("Unable to generate tmpFile. IOException: " + ioe.getMessage());
        } catch (SecurityException se) {
            logFileHashStore.error("FileHashStore.generateTmpFile - Unable to generate tmpFile: " + se.getMessage());
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
            boolean algorithmSupported = this.validateAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                throw new IllegalArgumentException(
                        "Algorithm not supported. Supported algorithms: " + Arrays.toString(SUPPORTED_HASH_ALGORITHMS));
            }
        }

        MessageDigest extraAlgo = null;
        Map<String, String> hexDigests = new HashMap<>();

        FileOutputStream os = new FileOutputStream(tmpFile);
        // Replace `_` with `-` in order to get valid MessageDigest objects
        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.name());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.name().replace("_", "-"));
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.name().replace("_", "-"));
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.name().replace("_", "-"));
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.name().replace("_", "-"));
        if (additionalAlgorithm != null) {
            logFileHashStore.info(
                    "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding additional algorithm to hex digest map, algorithm: "
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
            logFileHashStore.error(
                    "FileHashStore.writeToTmpFileAndGenerateChecksums - IOException encountered (os.flush/close or write related): "
                            + ioe.getMessage());
            throw ioe;
        } finally {
            os.flush();
            os.close();
        }

        logFileHashStore.info("FileHashStore.writeToTmpFileAndGenerateChecksums - Object has been written to tmpFile");
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
            logFileHashStore.error(
                    "FileHashStore.move - StandardCopyOption.ATOMIC_MOVE failed. AtomicMove is not supported across file systems. Source: "
                            + source + ". Target: " + target);
            throw amnse;
        } catch (IOException ioe) {
            logFileHashStore.error("FileHashStore.move - Unable to move file. Source: " + source
                    + ". Target: " + target);
            throw ioe;
        }
    }
}
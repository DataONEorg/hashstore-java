package org.dataone.hashstore.filehashstore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.HashStore;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.OrphanPidRefsFileException;
import org.dataone.hashstore.exceptions.OrphanRefsFilesException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.exceptions.UnsupportedHashAlgorithmException;

/**
 * FileHashStore is a HashStore adapter class that manages the storage of objects and metadata to a
 * given store path on disk. To instantiate FileHashStore, the calling app must provide predefined
 * properties as described by FileHashStore's single constructor.
 */
public class FileHashStore implements HashStore {
    private static final Log logFileHashStore = LogFactory.getLog(FileHashStore.class);
    private static final int TIME_OUT_MILLISEC = 1000;
    private static final ArrayList<String> objectLockedIds = new ArrayList<>(100);
    private static final ArrayList<String> metadataLockedIds = new ArrayList<>(100);
    private static final ArrayList<String> referenceLockedCids = new ArrayList<>(100);
    private final Path STORE_ROOT;
    private final int DIRECTORY_DEPTH;
    private final int DIRECTORY_WIDTH;
    private final String OBJECT_STORE_ALGORITHM;
    private final Path OBJECT_STORE_DIRECTORY;
    private final Path OBJECT_TMP_FILE_DIRECTORY;
    private final String DEFAULT_METADATA_NAMESPACE;
    private final Path METADATA_STORE_DIRECTORY;
    private final Path METADATA_TMP_FILE_DIRECTORY;
    private final Path REFS_STORE_DIRECTORY;
    private final Path REFS_TMP_FILE_DIRECTORY;
    private final Path REFS_PID_FILE_DIRECTORY;
    private final Path REFS_CID_FILE_DIRECTORY;

    public static final String HASHSTORE_YAML = "hashstore.yaml";

    public static final String[] SUPPORTED_HASH_ALGORITHMS = {"MD2", "MD5", "SHA-1", "SHA-256",
        "SHA-384", "SHA-512", "SHA-512/224", "SHA-512/256"};

    public enum HashStoreIdTypes {

        cid("cid"), pid("pid");

        final String identifierType;

        HashStoreIdTypes(String idType) {
            identifierType = idType;
        }

        public String getName() {
            return identifierType;
        }
    }

    enum DefaultHashAlgorithms {
        MD5("MD5"), SHA_1("SHA-1"), SHA_256("SHA-256"), SHA_384("SHA-384"), SHA_512("SHA-512");

        final String algoName;

        DefaultHashAlgorithms(String algo) {
            algoName = algo;
        }

        public String getName() {
            return algoName;
        }
    }

    enum HashStoreProperties {
        storePath, storeDepth, storeWidth, storeAlgorithm, storeMetadataNamespace
    }

    /**
     * Constructor to initialize HashStore, properties are required.
     *
     * Note: HashStore is not responsible for ensuring that the given store path is accurate. It
     * will only check for an existing configuration, directories or objects at the supplied store
     * path before initializing.
     *
     * @param hashstoreProperties Properties object with the following keys: storePath, storeDepth,
     *                            storeWidth, storeAlgorithm, storeMetadataNamespace
     * @throws IllegalArgumentException Constructor arguments cannot be null, empty or less than 0
     * @throws IOException              Issue with creating directories
     * @throws NoSuchAlgorithmException Unsupported store algorithm
     */
    public FileHashStore(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        logFileHashStore.info("FileHashStore - Call received to instantiate FileHashStore");
        FileHashStoreUtility.ensureNotNull(
            hashstoreProperties, "hashstoreProperties", "FileHashStore - constructor"
        );

        // Get properties
        // Note - Paths.get() throws NullPointerException if arg is null
        Path storePath = Paths.get(
            hashstoreProperties.getProperty(HashStoreProperties.storePath.name())
        );
        int storeDepth = Integer.parseInt(
            hashstoreProperties.getProperty(HashStoreProperties.storeDepth.name())
        );
        int storeWidth = Integer.parseInt(
            hashstoreProperties.getProperty(HashStoreProperties.storeWidth.name())
        );
        String storeAlgorithm = hashstoreProperties.getProperty(
            HashStoreProperties.storeAlgorithm.name()
        );
        String storeMetadataNamespace = hashstoreProperties.getProperty(
            HashStoreProperties.storeMetadataNamespace.name()
        );

        // Check given properties and/with existing HashStore
        verifyHashStoreProperties(
            storePath, storeDepth, storeWidth, storeAlgorithm, storeMetadataNamespace
        );

        // HashStore configuration has been reviewed, proceed with initialization
        STORE_ROOT = storePath;
        DIRECTORY_DEPTH = storeDepth;
        DIRECTORY_WIDTH = storeWidth;
        OBJECT_STORE_ALGORITHM = storeAlgorithm;
        DEFAULT_METADATA_NAMESPACE = storeMetadataNamespace;
        // Resolve object/metadata/refs directories
        OBJECT_STORE_DIRECTORY = storePath.resolve("objects");
        METADATA_STORE_DIRECTORY = storePath.resolve("metadata");
        REFS_STORE_DIRECTORY = storePath.resolve("refs");
        // Resolve tmp object/metadata directory paths, this is where objects are
        // created before they are moved to their permanent address
        OBJECT_TMP_FILE_DIRECTORY = OBJECT_STORE_DIRECTORY.resolve("tmp");
        METADATA_TMP_FILE_DIRECTORY = METADATA_STORE_DIRECTORY.resolve("tmp");
        REFS_TMP_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("tmp");
        REFS_PID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("pids");
        REFS_CID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("cids");

        try {
            // Physically create object & metadata store and tmp directories
            Files.createDirectories(OBJECT_STORE_DIRECTORY);
            Files.createDirectories(METADATA_STORE_DIRECTORY);
            Files.createDirectories(REFS_STORE_DIRECTORY);
            Files.createDirectories(OBJECT_TMP_FILE_DIRECTORY);
            Files.createDirectories(METADATA_TMP_FILE_DIRECTORY);
            Files.createDirectories(REFS_TMP_FILE_DIRECTORY);
            Files.createDirectories(REFS_PID_FILE_DIRECTORY);
            Files.createDirectories(REFS_CID_FILE_DIRECTORY);
            logFileHashStore.debug("FileHashStore - Created store and store tmp directories.");

        } catch (IOException ioe) {
            logFileHashStore.fatal(
                "FileHashStore - Failed to initialize FileHashStore - unable to create"
                    + " directories. Exception: " + ioe.getMessage()
            );
            throw ioe;
        }
        logFileHashStore.debug(
            "FileHashStore - HashStore initialized. Store Depth: " + DIRECTORY_DEPTH
                + ". Store Width: " + DIRECTORY_WIDTH + ". Store Algorithm: "
                + OBJECT_STORE_ALGORITHM + ". Store Metadata Namespace: "
                + DEFAULT_METADATA_NAMESPACE
        );

        // Write configuration file 'hashstore.yaml' to store HashStore properties
        Path hashstoreYaml = STORE_ROOT.resolve(HASHSTORE_YAML);
        if (!Files.exists(hashstoreYaml)) {
            String hashstoreYamlContent = buildHashStoreYamlString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, OBJECT_STORE_ALGORITHM, DEFAULT_METADATA_NAMESPACE
            );
            writeHashStoreYaml(hashstoreYamlContent);
            logFileHashStore.info(
                "FileHashStore - 'hashstore.yaml' written to storePath: " + hashstoreYaml
            );
        } else {
            logFileHashStore.info(
                "FileHashStore - 'hashstore.yaml' exists and has been verified."
                    + " Initializing FileHashStore."
            );
        }
    }

    // Configuration and Initialization Related Methods

    /**
     * Determines whether FileHashStore can instantiate by validating a set of arguments and
     * throwing exceptions. HashStore will not instantiate if an existing configuration file's
     * properties (`hashstore.yaml`) are different from what is supplied - or if an object store
     * exists at the given path, but it is missing the `hashstore.yaml` config file.
     *
     * If `hashstore.yaml` exists, it will retrieve its properties and compare them with the given
     * values; and if there is a mismatch, an exception will be thrown. If not, it will look to see
     * if any directories/files exist in the given store path and throw an exception if any file or
     * directory is found.
     *
     * @param storePath              Path where HashStore will store objects
     * @param storeDepth             Depth of directories
     * @param storeWidth             Width of directories
     * @param storeAlgorithm         Algorithm to use when calculating object addresses
     * @param storeMetadataNamespace Default metadata namespace (`formatId`)
     * @throws NoSuchAlgorithmException If algorithm supplied is not supported
     * @throws IOException              If `hashstore.yaml` config file cannot be retrieved/opened
     * @throws IllegalArgumentException If depth or width is less than 0
     * @throws IllegalStateException    If dirs/objects exist, but HashStore config is missing
     */
    protected void verifyHashStoreProperties(
        Path storePath, int storeDepth, int storeWidth, String storeAlgorithm,
        String storeMetadataNamespace
    ) throws NoSuchAlgorithmException, IOException, IllegalArgumentException, IllegalStateException {
        if (storeDepth <= 0 || storeWidth <= 0) {
            String errMsg = "FileHashStore - Depth and width must be greater than 0." + " Depth: "
                + storeDepth + ". Width: " + storeWidth;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        // Ensure algorithm supplied is not empty, not null and supported
        validateAlgorithm(storeAlgorithm);
        // Review metadata format (formatId)
        FileHashStoreUtility.ensureNotNull(
            storeMetadataNamespace, "storeMetadataNamespace", "FileHashStore - constructor"
        );
        FileHashStoreUtility.checkForEmptyString(
            storeMetadataNamespace, "storeMetadataNamespace", "FileHashStore - constructor"
        );

        // Check to see if configuration exists before initializing
        Path hashstoreYamlPredictedPath = Paths.get(storePath + "/hashstore.yaml");
        if (Files.exists(hashstoreYamlPredictedPath)) {
            logFileHashStore.debug("FileHashStore - 'hashstore.yaml' found, verifying properties.");

            HashMap<String, Object> hsProperties = loadHashStoreYaml(storePath);
            int existingStoreDepth = (int) hsProperties.get(HashStoreProperties.storeDepth.name());
            int existingStoreWidth = (int) hsProperties.get(HashStoreProperties.storeWidth.name());
            String existingStoreAlgorithm = (String) hsProperties.get(
                HashStoreProperties.storeAlgorithm.name()
            );
            String existingStoreMetadataNs = (String) hsProperties.get(
                HashStoreProperties.storeMetadataNamespace.name()
            );

            // Verify properties when 'hashstore.yaml' found
            FileHashStoreUtility.checkObjectEquality("store depth", storeDepth, existingStoreDepth);
            FileHashStoreUtility.checkObjectEquality("store width", storeWidth, existingStoreWidth);
            FileHashStoreUtility.checkObjectEquality("store algorithm", storeAlgorithm,
                                                     existingStoreAlgorithm);
            FileHashStoreUtility.checkObjectEquality(
                "store metadata namespace", storeMetadataNamespace, existingStoreMetadataNs);

        } else {
            // Check if HashStore exists at the given store path (and is missing config)
            logFileHashStore.debug(
                "FileHashStore - 'hashstore.yaml' not found, check store path for"
                    + " objects and directories."
            );

            if (Files.isDirectory(storePath)) {
                if (FileHashStoreUtility.dirContainsFiles(storePath)) {
                    String errMsg = "FileHashStore - Missing 'hashstore.yaml' but directories"
                        + " and/or objects found.";
                    logFileHashStore.fatal(errMsg);
                    throw new IllegalStateException(errMsg);

                }
            }
            logFileHashStore.debug(
                "FileHashStore - 'hashstore.yaml' not found and store path"
                    + " not yet initialized."
            );
        }
    }

    /**
     * Get the properties of HashStore from 'hashstore.yaml'
     *
     * @param storePath Path to root of store
     * @return HashMap of the properties
     * @throws IOException If `hashstore.yaml` doesn't exist
     */
    protected HashMap<String, Object> loadHashStoreYaml(Path storePath) throws IOException {
        Path hashStoreYamlPath = storePath.resolve(HASHSTORE_YAML);
        File hashStoreYamlFile = hashStoreYamlPath.toFile();
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        HashMap<String, Object> hsProperties = new HashMap<>();

        try {
            HashMap<?, ?> hashStoreYamlProperties = om.readValue(hashStoreYamlFile, HashMap.class);
            hsProperties.put(
                HashStoreProperties.storeDepth.name(), hashStoreYamlProperties.get("store_depth")
            );
            hsProperties.put(
                HashStoreProperties.storeWidth.name(), hashStoreYamlProperties.get("store_width")
            );
            hsProperties.put(
                HashStoreProperties.storeAlgorithm.name(), hashStoreYamlProperties.get(
                    "store_algorithm"
                )
            );
            hsProperties.put(
                HashStoreProperties.storeMetadataNamespace.name(), hashStoreYamlProperties.get(
                    "store_metadata_namespace"
                )
            );

        } catch (IOException ioe) {
            logFileHashStore.fatal(
                "FileHashStore.getHashStoreYaml() - Unable to retrieve 'hashstore.yaml'."
                    + " IOException: " + ioe.getMessage()
            );
            throw ioe;
        }

        return hsProperties;
    }

    /**
     * Write a 'hashstore.yaml' file to STORE_ROOT
     *
     * @param yamlString Content of the HashStore configuration
     * @throws IOException If unable to write `hashstore.yaml`
     */
    protected void writeHashStoreYaml(String yamlString) throws IOException {
        Path hashstoreYaml = STORE_ROOT.resolve(HASHSTORE_YAML);

        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(Files.newOutputStream(hashstoreYaml), StandardCharsets.UTF_8)
        )) {
            writer.write(yamlString);

        } catch (IOException ioe) {
            logFileHashStore.fatal(
                "FileHashStore.writeHashStoreYaml() - Unable to write 'hashstore.yaml'."
                    + " IOException: " + ioe.getMessage()
            );
            throw ioe;
        }
    }

    /**
     * Build the string content of the configuration file for HashStore - 'hashstore.yaml'
     *
     * @param storeDepth             Depth of store
     * @param storeWidth             Width of store
     * @param storeAlgorithm         Algorithm to use to calculate the hex digest for the permanent
     *                               address of a data object
     * @param storeMetadataNamespace default formatId of hashstore metadata
     * @return String that representing the contents of 'hashstore.yaml'
     */
    protected String buildHashStoreYamlString(
        int storeDepth, int storeWidth, String storeAlgorithm, String storeMetadataNamespace
    ) {

        return String.format(
            "# Default configuration variables for HashStore\n\n"
                + "############### Directory Structure ###############\n"
                + "# Desired amount of directories when sharding an object to "
                + "form the permanent address\n"
                + "store_depth: %d  # WARNING: DO NOT CHANGE UNLESS SETTING UP " + "NEW HASHSTORE\n"
                + "# Width of directories created when sharding an object to "
                + "form the permanent address\n"
                + "store_width: %d  # WARNING: DO NOT CHANGE UNLESS SETTING UP " + "NEW HASHSTORE\n"
                + "# Example:\n" + "# Below, objects are shown listed in directories that are # "
                + "levels deep (DIR_DEPTH=3),\n"
                + "# with each directory consisting of 2 characters " + "(DIR_WIDTH=2).\n"
                + "#    /var/filehashstore/objects\n" + "#    ├── 7f\n" + "#    │   └── 5c\n"
                + "#    │       └── c1\n" + "#    │           └── "
                + "8f0b04e812a3b4c8f686ce34e6fec558804bf61e54b176742a7f6368d6\n\n"
                + "############### Format of the Metadata ###############\n"
                + "store_sysmeta_namespace: \"http://ns.dataone" + ".org/service/types/v2.0\"\n\n"
                + "############### Hash Algorithms ###############\n"
                + "# Hash algorithm to use when calculating object's hex digest "
                + "for the permanent address\n" + "store_algorithm: \"%s\"\n"
                + "############### Hash Algorithms ###############\n"
                + "# Hash algorithm to use when calculating object's hex digest "
                + "for the permanent address\n" + "store_metadata_namespace: \"%s\"\n"
                + "# The default algorithm list includes the hash algorithms "
                + "calculated when storing an\n"
                + "# object to disk and returned to the caller after successful " + "storage.\n"
                + "store_default_algo_list:\n" + "- \"MD5\"\n" + "- \"SHA-1\"\n" + "- \"SHA-256\"\n"
                + "- \"SHA-384\"\n" + "- \"SHA-512\"\n", storeDepth, storeWidth, storeAlgorithm,
            storeMetadataNamespace
        );
    }

    // HashStore Public API Methods

    @Override
    public ObjectMetadata storeObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws NoSuchAlgorithmException, IOException, RuntimeException, InterruptedException,
        PidRefsFileExistsException {
        logFileHashStore.debug(
            "FileHashStore.storeObject - Called to store object for pid: " + pid
        );

        // Begin input validation
        FileHashStoreUtility.ensureNotNull(object, "object", "storeObject");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "storeObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "storeObject");
        // Validate algorithms if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                additionalAlgorithm, "additionalAlgorithm", "storeObject"
            );
            validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "checksumAlgorithm", "storeObject"
            );
            validateAlgorithm(checksumAlgorithm);
        }
        if (objSize != -1) {
            FileHashStoreUtility.checkNotNegativeOrZero(objSize, "storeObject");
        }

        return syncPutObject(
            object, pid, additionalAlgorithm, checksum, checksumAlgorithm, objSize
        );
    }

    /**
     * Method to synchronize storing objects with FileHashStore
     */
    private ObjectMetadata syncPutObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws NoSuchAlgorithmException, PidRefsFileExistsException, IOException, RuntimeException,
        InterruptedException {
        // Lock pid for thread safety, transaction control and atomic writing
        // An object is stored once and only once
        synchronized (objectLockedIds) {
            if (objectLockedIds.contains(pid)) {
                String errMsg =
                    "FileHashStore.syncPutObject - Duplicate object request encountered for pid: "
                        + pid + ". Already in progress.";
                logFileHashStore.warn(errMsg);
                throw new RuntimeException(errMsg);
            }
            logFileHashStore.debug(
                "FileHashStore.storeObject - Synchronizing objectLockedIds for pid: " + pid
            );
            objectLockedIds.add(pid);
        }

        try {
            logFileHashStore.debug(
                "FileHashStore.syncPutObject - called .putObject() to store pid: " + pid
                    + ". additionalAlgorithm: " + additionalAlgorithm + ". checksum: " + checksum
                    + ". checksumAlgorithm: " + checksumAlgorithm
            );
            // Store object
            ObjectMetadata objInfo = putObject(
                object, pid, additionalAlgorithm, checksum, checksumAlgorithm, objSize
            );
            // Tag object
            String cid = objInfo.getCid();
            tagObject(pid, cid);
            logFileHashStore.info(
                "FileHashStore.syncPutObject - Object stored for pid: " + pid
                    + ". Permanent address: " + getExpectedPath(pid, "object", null)
            );
            objInfo.setPid(pid);
            return objInfo;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". NoSuchAlgorithmException: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } catch (PidRefsFileExistsException prfee) {
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". PidRefsFileExistsException: " + prfee.getMessage();
            logFileHashStore.error(errMsg);
            throw prfee;

        } catch (IOException ioe) {
            // Covers AtomicMoveNotSupportedException, FileNotFoundException
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (RuntimeException re) {
            // Covers SecurityException, IllegalArgumentException, NullPointerException
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". Runtime Exception: " + re.getMessage();
            logFileHashStore.error(errMsg);
            throw re;

        } finally {
            // Release lock
            synchronized (objectLockedIds) {
                logFileHashStore.debug(
                    "FileHashStore.syncPutObject - Releasing objectLockedIds for pid: " + pid
                );
                objectLockedIds.remove(pid);
                objectLockedIds.notify();
            }
        }
    }

    /**
     * Overload method for storeObject with just an InputStream
     */
    @Override
    public ObjectMetadata storeObject(InputStream object) throws NoSuchAlgorithmException,
        IOException, PidRefsFileExistsException, RuntimeException {
        // 'putObject' is called directly to bypass the pid synchronization implemented to
        // efficiently handle object store requests without a pid. This scenario occurs when
        // metadata about the object (ex. form data including the pid, checksum, checksum
        // algorithm, etc.) is unavailable.
        //
        // Note: This method does not tag the object to make it discoverable, so the client can
        // call 'verifyObject' (optional) to check that the object is valid, and 'tagObject'
        // (required) to create the reference files needed to associate the respective pids/cids.
        return putObject(object, "HashStoreNoPid", null, null, null, -1);
    }


    /**
     * Overload method for storeObject with size and a checksum & checksumAlgorithm.
     */
    @Override
    public ObjectMetadata storeObject(
        InputStream object, String pid, String checksum, String checksumAlgorithm, long objSize
    ) throws NoSuchAlgorithmException, IOException, PidRefsFileExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "storeObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "storeObject");
        FileHashStoreUtility.checkNotNegativeOrZero(objSize, "storeObject");

        return storeObject(object, pid, null, checksum, checksumAlgorithm, objSize);
    }

    /**
     * Overload method for storeObject with just a checksum and checksumAlgorithm
     */
    @Override
    public ObjectMetadata storeObject(
        InputStream object, String pid, String checksum, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, PidRefsFileExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "storeObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "storeObject");

        return storeObject(object, pid, null, checksum, checksumAlgorithm, -1);
    }

    /**
     * Overload method for storeObject with just the size of object to validate
     */
    @Override
    public ObjectMetadata storeObject(InputStream object, String pid, long objSize)
        throws NoSuchAlgorithmException, IOException, PidRefsFileExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.checkNotNegativeOrZero(objSize, "storeObject");

        return storeObject(object, pid, null, null, null, objSize);
    }

    /**
     * Overload method for storeObject with an additionalAlgorithm
     */
    @Override
    public ObjectMetadata storeObject(InputStream object, String pid, String additionalAlgorithm)
        throws NoSuchAlgorithmException, IOException, PidRefsFileExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.ensureNotNull(
            additionalAlgorithm, "additionalAlgorithm", "storeObject"
        );

        return storeObject(object, pid, additionalAlgorithm, null, null, -1);
    }


    @Override
    public void tagObject(String pid, String cid) throws IOException, PidRefsFileExistsException,
        NoSuchAlgorithmException, FileNotFoundException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.tagObject - Called to tag cid (" + cid + ") with pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "tagObject");
        FileHashStoreUtility.ensureNotNull(cid, "cid", "tagObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "tagObject");
        FileHashStoreUtility.checkForEmptyString(cid, "cid", "tagObject");

        // tagObject is synchronized with deleteObject based on a `cid`
        synchronized (referenceLockedCids) {
            while (referenceLockedCids.contains(cid)) {
                try {
                    referenceLockedCids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.tagObject - referenceLockedCids lock was interrupted while"
                            + " waiting to tag pid: " + pid + " with cid: " + cid
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.tagObject - Synchronizing referenceLockedCids for pid: " + pid
            );
            referenceLockedCids.add(cid);
        }

        try {
            // Prepare booleans to determine path of tagObject to proceed with
            Path absPidRefsPath = getExpectedPath(pid, "refs", HashStoreIdTypes.pid.getName());
            Path absCidRefsPath = getExpectedPath(cid, "refs", HashStoreIdTypes.cid.getName());
            boolean pidRefsFound = Files.exists(absPidRefsPath);
            boolean cidRefsFound = Files.exists(absCidRefsPath);

            // Both files found, confirm that reference files are where they are expected to be
            if (pidRefsFound && cidRefsFound) {
                verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
                logFileHashStore.info(
                    "FileHashStore.tagObject - Object with cid: " + cid
                        + " already exists and is tagged with pid: " + pid
                );
            } else if (pidRefsFound && !cidRefsFound) {
                // If pid refs exists, it can only contain and reference one cid
                // First, compare the cid retrieved from the pid refs file from the supplied cid
                String retrievedCid = new String(Files.readAllBytes(absPidRefsPath));
                if (retrievedCid.equalsIgnoreCase(cid)) {
                    // The pid correctly references the cid, but the cid refs file is missing
                    // Create the file and verify tagging process
                    File cidRefsTmpFile = writeRefsFile(pid, HashStoreIdTypes.cid.getName());
                    File absPathCidRefsFile = absCidRefsPath.toFile();
                    move(cidRefsTmpFile, absPathCidRefsFile, "refs");
                    verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
                    logFileHashStore.info(
                        "FileHashStore.tagObject - Pid refs file exists for pid: " + pid
                            + ", but cid refs file for: " + cid
                            + " is missing. Missing cid refs file created and tagging completed."
                    );
                    return;
                } else {
                    // Check if the retrieved cid refs file exists and pid is referenced
                    Path retrievedAbsCidRefsPath = getExpectedPath(
                        retrievedCid, "refs", HashStoreIdTypes.cid.getName()
                    );
                    boolean retrievedAbsCidRefsPathExists = Files.exists(retrievedAbsCidRefsPath);
                    if (retrievedAbsCidRefsPathExists && isStringInRefsFile(
                        pid, retrievedAbsCidRefsPath
                    )) {
                        // This pid is accounted for and tagged as expected.
                        String errMsg =
                            "FileHashStore.tagObject - pid refs file already exists for pid: " + pid
                                + ", and the associated cid refs file contains the pid."
                                + " A pid can only reference one cid.";
                        logFileHashStore.error(errMsg);
                        throw new PidRefsFileExistsException(errMsg);
                    }
                    // Orphaned pid refs file found, the retrieved cid refs file exists
                    // but doesn't contain the cid. Proceed to overwrite the pid refs file.
                    // There is no return statement, so we move out of this if block.
                }
            } else if (!pidRefsFound && cidRefsFound) {
                // Only update cid refs file if pid is not in the file
                boolean pidFoundInCidRefFiles = isStringInRefsFile(pid, absCidRefsPath);
                if (!pidFoundInCidRefFiles) {
                    updateRefsFile(pid, absCidRefsPath, "add");
                }
                // Get the pid refs file and verify tagging process
                File pidRefsTmpFile = writeRefsFile(cid, HashStoreIdTypes.pid.getName());
                File absPathPidRefsFile = absPidRefsPath.toFile();
                move(pidRefsTmpFile, absPathPidRefsFile, "refs");
                verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
                logFileHashStore.info(
                    "FileHashStore.tagObject - Object with cid: " + cid
                        + " has been updated and tagged successfully with pid: " + pid
                );
                return;
            }

            // Get pid and cid refs files 
            File pidRefsTmpFile = writeRefsFile(cid, HashStoreIdTypes.pid.getName());
            File cidRefsTmpFile = writeRefsFile(pid, HashStoreIdTypes.cid.getName());
            // Move refs files to permanent location
            File absPathPidRefsFile = absPidRefsPath.toFile();
            File absPathCidRefsFile = absCidRefsPath.toFile();
            move(pidRefsTmpFile, absPathPidRefsFile, "refs");
            move(cidRefsTmpFile, absPathCidRefsFile, "refs");
            // Verify tagging process, this throws exceptions if there's an issue
            verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
            logFileHashStore.info(
                "FileHashStore.tagObject - Object with cid: " + cid
                    + " has been tagged successfully with pid: " + pid
            );

        } finally {
            // Release lock
            synchronized (referenceLockedCids) {
                logFileHashStore.debug(
                    "FileHashStore.tagObject - Releasing referenceLockedCids for pid: " + pid
                        + " with cid: " + cid);
                referenceLockedCids.remove(cid);
                referenceLockedCids.notify();
            }
        }
    }

    @Override
    public void verifyObject(
        ObjectMetadata objectInfo, String checksum, String checksumAlgorithm, long objSize
    ) throws NonMatchingObjSizeException, NonMatchingChecksumException,
        UnsupportedHashAlgorithmException, IOException {
        logFileHashStore.debug(
            "FileHashStore.verifyObject - Called to verify object with id: " + objectInfo.getCid()
        );
        FileHashStoreUtility.ensureNotNull(objectInfo, "objectInfo", "verifyObject");
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "verifyObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "verifyObject");
        FileHashStoreUtility.checkNotNegativeOrZero(objSize, "verifyObject");

        String objCid = objectInfo.getCid();
        long objInfoRetrievedSize = objectInfo.getSize();
        Map<String, String> hexDigests = objectInfo.getHexDigests();
        String digestFromHexDigests = hexDigests.get(checksumAlgorithm);

        // Confirm that requested checksum to verify against is available
        if (digestFromHexDigests == null) {
            try {
                validateAlgorithm(checksumAlgorithm);
                // If no exceptions thrown, calculate the checksum with the given algo
                String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, objCid
                );
                Path pathToCidObject = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
                try (InputStream inputStream = Files.newInputStream(pathToCidObject)) {
                    digestFromHexDigests = FileHashStoreUtility.calculateHexDigest(inputStream,
                        checksumAlgorithm);
                } catch (IOException ioe) {
                    String errMsg =
                        "FileHashStore.verifyObject - Unexpected error when calculating a checksum"
                            + " for cid: " + objCid + " with algorithm (" + checksumAlgorithm
                        + ") that is not part of the default list. " + ioe.getMessage();
                    throw new IOException(errMsg);
                }
            } catch (NoSuchAlgorithmException nsae) {
                String errMsg =
                    "FileHashStore.verifyObject - checksumAlgorithm given: " + checksumAlgorithm
                        + " is not supported. Supported algorithms: " + Arrays.toString(
                        SUPPORTED_HASH_ALGORITHMS
                    );
                logFileHashStore.error(errMsg);
                throw new UnsupportedHashAlgorithmException(errMsg);
            }
        }
        // Validate checksum
        if (!digestFromHexDigests.equals(checksum)) {
            String errMsg = "FileHashStore.verifyObject - Object content invalid for cid: " + objCid
                + ". Expected checksum: " + checksum + ". Actual checksum calculated: "
                + digestFromHexDigests + " (algorithm: " + checksumAlgorithm + ")";
            logFileHashStore.error(errMsg);
            throw new NonMatchingChecksumException(errMsg);
        }
        // Validate size
        if (objInfoRetrievedSize != objSize) {
            String errMsg = "FileHashStore.verifyObject - Object size invalid for cid: " + objCid
                + ". Expected size: " + objSize + ". Actual size: " + objInfoRetrievedSize;
            logFileHashStore.error(errMsg);
            throw new NonMatchingObjSizeException(errMsg);
        }

        String infoMsg = "FileHashStore.verifyObject - Object has been validated for cid: " + objCid
                + ". Expected checksum: " + checksum + ". Actual checksum calculated: "
                + digestFromHexDigests + " (algorithm: " + checksumAlgorithm + ")";
        logFileHashStore.info(infoMsg);
    }

    @Override
    public String findObject(String pid) throws NoSuchAlgorithmException, IOException,
        OrphanPidRefsFileException, PidNotFoundInCidRefsFileException, OrphanRefsFilesException {
        logFileHashStore.debug("FileHashStore.findObject - Called to find object for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(pid, "pid", "findObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "findObject");

        // Get path of the pid references file
        Path absPidRefsPath = getExpectedPath(pid, "refs", HashStoreIdTypes.pid.getName());

        if (Files.exists(absPidRefsPath)) {
            String cid = new String(Files.readAllBytes(absPidRefsPath));
            Path absCidRefsPath = getExpectedPath(cid, "refs", HashStoreIdTypes.cid.getName());

            // Throw exception if the cid refs file doesn't exist
            if (!Files.exists(absCidRefsPath)) {
                String errMsg =
                    "FileHashStore.findObject - Cid refs file does not exist for cid: " + cid
                        + " with address: " + absCidRefsPath + ", but pid refs file exists.";
                logFileHashStore.error(errMsg);
                throw new OrphanPidRefsFileException(errMsg);
            }
            // If the pid is found in the expected cid refs file, and the object exists, return it
            if (isStringInRefsFile(pid, absCidRefsPath)) {
                logFileHashStore.info(
                    "FileHashStore.findObject - Cid (" + cid + ") found for pid: " + pid
                );

                String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, cid
                );
                Path realPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
                if (Files.exists(realPath)) {
                    return cid;

                } else {
                    String errMsg = "FileHashStore.findObject - Object with cid: " + cid
                        + " does not exist, but pid and cid reference file found for pid: " + pid;
                    logFileHashStore.error(errMsg);
                    throw new OrphanRefsFilesException(errMsg);
                }

            } else {
                String errMsg = "FileHashStore.findObject - Pid refs file exists, but pid (" + pid
                    + ") not found in cid refs file for cid: " + cid + " with address: "
                    + absCidRefsPath;
                logFileHashStore.error(errMsg);
                throw new PidNotFoundInCidRefsFileException(errMsg);
            }

        } else {
            String errMsg = "FileHashStore.findObject - Unable to find cid for pid: " + pid
                + ". Pid refs file does not exist at: " + absPidRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
    }

    @Override
    public String storeMetadata(InputStream metadata, String pid, String formatId)
        throws IOException, IllegalArgumentException, FileNotFoundException, InterruptedException,
        NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.storeMetadata - Called to store metadata for pid: " + pid
                + ", with formatId: " + formatId
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(metadata, "metadata", "storeMetadata");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "storeMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "storeMetadata");

        // Determine metadata namespace
        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = DEFAULT_METADATA_NAMESPACE;
        } else {
            FileHashStoreUtility.checkForEmptyString(formatId, "formatId", "storeMetadata");
            checkedFormatId = formatId;
        }

        return syncPutMetadata(metadata, pid, checkedFormatId);
    }

    /**
     * Method to synchronize storing metadata with FileHashStore
     */
    private String syncPutMetadata(InputStream metadata, String pid, String checkedFormatId)
        throws InterruptedException, IOException, NoSuchAlgorithmException {
        // Lock pid for thread safety, transaction control and atomic writing
        // Metadata storage requests for the same pid must be written serially
        // However, the same pid could be used with different formatIds, so
        // synchronize ids with pid + formatId;
        String pidFormatId = pid + checkedFormatId;
        String metadataDocId = FileHashStoreUtility.getPidHexDigest(pidFormatId,
                                                                OBJECT_STORE_ALGORITHM);
        synchronized (metadataLockedIds) {
            while (metadataLockedIds.contains(metadataDocId)) {
                try {
                    metadataLockedIds.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.storeMetadata - Metadata lock was interrupted while"
                            + " storing metadata for: " + pid + " and formatId: " + checkedFormatId
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.storeMetadata - Synchronizing metadataLockedIds for pid: " + pid
            );
            metadataLockedIds.add(metadataDocId);
        }

        try {
            logFileHashStore.debug(
                "FileHashStore.storeMetadata - .putMetadata() request for pid: " + pid
                    + ". formatId: " + checkedFormatId
            );
            // Store metadata
            String pathToStoredMetadata = putMetadata(metadata, pid, checkedFormatId);
            logFileHashStore.info(
                "FileHashStore.storeMetadata - Metadata stored for pid: " + pid + " at: "
                    + pathToStoredMetadata
            );
            return pathToStoredMetadata;

        } catch (IOException ioe) {
            // Covers FileNotFoundException
            String errMsg = "FileHashStore.storeMetadata - Unable to store metadata, IOException"
                + " encountered: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg =
                "FileHashStore.storeMetadata - Unable to store metadata, algorithm to calculate"
                    + " permanent address is not supported: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } finally {
            // Release lock
            synchronized (metadataLockedIds) {
                logFileHashStore.debug(
                    "FileHashStore.storeMetadata - Releasing metadataLockedIds for pid: " + pid
                        + " and formatId " + checkedFormatId
                );
                metadataLockedIds.remove(metadataDocId);
                metadataLockedIds.notify();
            }
        }
    }

    /**
     * Overload method for storeMetadata with default metadata namespace
     */
    @Override
    public String storeMetadata(InputStream metadata, String pid) throws IOException,
        IllegalArgumentException, FileNotFoundException, InterruptedException,
        NoSuchAlgorithmException {
        return storeMetadata(metadata, pid, DEFAULT_METADATA_NAMESPACE);
    }

    @Override
    public InputStream retrieveObject(String pid) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.retrieveObject - Called to retrieve object for pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "retrieveObject");

        // Get permanent address of the pid by calculating its sha-256 hex digest
        Path objRealPath = getExpectedPath(pid, "object", null);

        // Check to see if object exists
        if (!Files.exists(objRealPath)) {
            String errMsg = "FileHashStore.retrieveObject - File does not exist for pid: " + pid
                + " with object address: " + objRealPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // If so, return an input stream for the object
        try {
            InputStream objectCidInputStream = Files.newInputStream(objRealPath);
            logFileHashStore.info(
                "FileHashStore.retrieveObject - Retrieved object for pid: " + pid
            );
            return objectCidInputStream;

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.retrieveObject - Unexpected error when creating InputStream"
                    + " for pid: " + pid + ", IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }

    }

    @Override
    public InputStream retrieveMetadata(String pid, String formatId)
        throws IllegalArgumentException, FileNotFoundException, IOException,
        NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.retrieveMetadata - Called to retrieve metadata for pid: " + pid
                + " with formatId: " + formatId
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.ensureNotNull(formatId, "formatId", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyString(formatId, "formatId", "retrieveMetadata");

        // Get permanent address of the pid by calculating its sha-256 hex digest
        Path metadataCidPath = getExpectedPath(pid, "metadata", formatId);

        // Check to see if metadata exists
        if (!Files.exists(metadataCidPath)) {
            String errMsg = "FileHashStore.retrieveMetadata - Metadata does not exist for pid: "
                + pid + " with formatId: " + formatId + ". Metadata address: " + metadataCidPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // If so, return an input stream for the metadata
        try {
            InputStream metadataCidInputStream = Files.newInputStream(metadataCidPath);
            logFileHashStore.info(
                "FileHashStore.retrieveMetadata - Retrieved metadata for pid: " + pid
                    + " with formatId: " + formatId
            );
            return metadataCidInputStream;

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.retrieveMetadata - Unexpected error when creating InputStream"
                    + " for pid: " + pid + " with formatId: " + formatId + ". IOException: " + ioe
                        .getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Overload method for retrieveMetadata with default metadata namespace
     */
    @Override
    public InputStream retrieveMetadata(String pid) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.retrieveMetadata - Called to retrieve metadata for pid: " + pid
                + " with default metadata namespace: " + DEFAULT_METADATA_NAMESPACE
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "retrieveMetadata");

        // Get permanent address of the pid by calculating its sha-256 hex digest
        Path metadataCidPath = getExpectedPath(pid, "metadata", DEFAULT_METADATA_NAMESPACE);

        // Check to see if metadata exists
        if (!Files.exists(metadataCidPath)) {
            String errMsg = "FileHashStore.retrieveMetadata - Metadata does not exist for pid: "
                + pid + " with formatId: " + DEFAULT_METADATA_NAMESPACE + ". Metadata address: "
                + metadataCidPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // If so, return an input stream for the metadata
        InputStream metadataCidInputStream;
        try {
            metadataCidInputStream = Files.newInputStream(metadataCidPath);
            logFileHashStore.info(
                "FileHashStore.retrieveMetadata - Retrieved metadata for pid: " + pid
                    + " with formatId: " + DEFAULT_METADATA_NAMESPACE
            );
        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.retrieveMetadata - Unexpected error when creating InputStream"
                    + " for pid: " + pid + " with formatId: " + DEFAULT_METADATA_NAMESPACE
                    + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }

        return metadataCidInputStream;
    }

    @Override
    public void deleteObject(String idType, String id) throws IllegalArgumentException, IOException,
        NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.deleteObject - Called to delete object for id: " + id + "(" + idType
                + ")");
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(id, "id", "deleteObject");
        FileHashStoreUtility.checkForEmptyString(id, "id", "deleteObject");
        if (!idType.equals(HashStoreIdTypes.pid.getName()) && !idType.equals(
            HashStoreIdTypes.cid.getName()

        )) {
            String errMsg = "FileHashStore.deleteObject - 'idType' must be 'pid' or 'cid'";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // If 'idType' is cid, attempt to delete the object directly without checking refs files
        if (idType.equals(HashStoreIdTypes.cid.getName())) {
            // The following method is synchronized based on cids
            deleteObjectByCid(id);

        } else {
            // Else 'idType' is pid
            String cid;
            String pid = id;
            List<Path> deleteList = new ArrayList<>();

            // Storing and deleting objects are synchronized together
            // Duplicate store object requests for a pid are rejected, but deleting an object
            // will wait for a pid to be released if it's found to be in use before proceeding.
            synchronized (objectLockedIds) {
                while (objectLockedIds.contains(pid)) {
                    try {
                        objectLockedIds.wait(TIME_OUT_MILLISEC);

                    } catch (InterruptedException ie) {
                        String errMsg =
                            "FileHashStore.deleteObject - Delete request for pid: " + pid
                                + " has been interrupted.";
                        logFileHashStore.warn(errMsg);
                        throw new InterruptedException(errMsg);
                    }
                }
                logFileHashStore.debug(
                    "FileHashStore.deleteObject - Synchronizing objectLockedIds for pid: " + pid);
                objectLockedIds.add(pid);
            }

            try {
                // Get list of metadata documents first, these will always be deleted if they exist
                // and reduces the time spent in the synchronization block. We have locked the `pid`
                // so we are safe to proceed with working on `pid` related metadata documents.
                // Metadata directory
                String pidHexDigest =
                    FileHashStoreUtility.getPidHexDigest(pid, OBJECT_STORE_ALGORITHM);
                String pidRelativePath =
                    FileHashStoreUtility.getHierarchicalPathString(DIRECTORY_DEPTH, DIRECTORY_WIDTH,
                                                                   pidHexDigest);

                try {
                    // Before we begin deletion process, we look for the `cid` by calling
                    // `findObject` which will throw custom exceptions if there is an issue with
                    // the reference files, which help us determine the path to proceed with.
                    cid = findObject(id);

                    // If no exceptions are thrown, we proceed to synchronization based on the `cid`
                    // Multiple threads may access the cid reference file (which contains a list of
                    // `pid`s that reference a `cid`) and this needs to be coordinated. Otherwise,
                    // we will run into a `OverlappingFileLockException`
                    synchronized (referenceLockedCids) {
                        while (referenceLockedCids.contains(cid)) {
                            try {
                                referenceLockedCids.wait(TIME_OUT_MILLISEC);

                            } catch (InterruptedException ie) {
                                String errMsg =
                                    "FileHashStore.deleteObject - referenceLockedCids lock was "
                                        + "interrupted while waiting to delete objects for pid: "
                                        + pid + ". InterruptedException: " + ie.getMessage();
                                logFileHashStore.error(errMsg);
                                throw new InterruptedException(errMsg);
                            }
                        }
                        logFileHashStore.debug(
                            "FileHashStore.deleteObject - Synchronizing referenceLockedCids for "
                            + "pid: "
                                + pid + " with cid: " + cid);
                        referenceLockedCids.add(cid);
                    }

                    try {
                        // Proceed with comprehensive deletion - cid exists, nothing out of place
                        // Get all the required paths to streamline deletion process
                        // Permanent address of the object
                        Path objRealPath = getExpectedPath(pid, "object", null);
                        // Cid refs file
                        Path absCidRefsPath =
                            getExpectedPath(cid, "refs", HashStoreIdTypes.cid.getName());
                        // Pid refs file
                        Path absPidRefsPath =
                            getExpectedPath(pid, "refs", HashStoreIdTypes.pid.getName());

                        // Rename pid refs file to prepare for deletion
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                        // Remove pid from cid refs file
                        updateRefsFile(pid, absCidRefsPath, "remove");
                        // Delete obj and cid refs file **only** if the cid refs file is empty
                        if (Files.size(absCidRefsPath) == 0) {
                            // Rename empty cid refs file to prepare for deletion
                            deleteList.add(
                                FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                            // Rename actual object to prepare for deletion
                            deleteList.add(FileHashStoreUtility.renamePathForDeletion(objRealPath));
                        } else {
                            String warnMsg =
                                "FileHashStore.deleteObject - cid referenced by pid: " + pid
                                    + " is not empty (refs exist for cid). Skipping object "
                                    + "deletion.";
                            logFileHashStore.warn(warnMsg);
                        }
                        // Delete all related/relevant items with the least amount of delay
                        FileHashStoreUtility.deleteListItems(deleteList);
                        // Remove metadata files
                        deleteMetadata(pid);
                        logFileHashStore.info(
                            "FileHashStore.deleteObject - File and references deleted for: " + pid
                                + " with object address: " + objRealPath);

                    } finally {
                        // Release lock
                        synchronized (referenceLockedCids) {
                            logFileHashStore.debug(
                                "FileHashStore.deleteObject - Releasing referenceLockedCids for "
                                + "pid: "
                                    + pid + " with cid: " + cid);
                            referenceLockedCids.remove(cid);
                            referenceLockedCids.notify();
                        }
                    }

                } catch (OrphanPidRefsFileException oprfe) {
                    // `findObject` throws this exception when the cid refs file doesn't exist,
                    // so we only need to delete the pid refs file and related metadata documents

                    // Begin by renaming pid refs file for deletion
                    Path absPidRefsPath =
                        getExpectedPath(pid, "refs", HashStoreIdTypes.pid.getName());
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete items
                    FileHashStoreUtility.deleteListItems(deleteList);
                    // Remove metadata files
                    deleteMetadata(pid);
                    String warnMsg =
                        "FileHashStore.deleteObject - Cid refs file does not exist for pid: " + pid
                            + ". Deleted orphan pid refs file and metadata.";
                    logFileHashStore.warn(warnMsg);

                } catch (OrphanRefsFilesException orfe) {
                    // `findObject` throws this exception when the pid and cid refs file exists,
                    // but the actual object being referenced by the pid does not exist

                    // Get the cid from the pid refs file before renaming it for deletion
                    Path absPidRefsPath =
                        getExpectedPath(id, "refs", HashStoreIdTypes.pid.getName());
                    String cidRead = new String(Files.readAllBytes(absPidRefsPath));

                    // Since we must access the cid reference file, the `cid` must be synchronized
                    synchronized (referenceLockedCids) {
                        while (referenceLockedCids.contains(cidRead)) {
                            try {
                                referenceLockedCids.wait(TIME_OUT_MILLISEC);

                            } catch (InterruptedException ie) {
                                String errMsg =
                                    "FileHashStore.deleteObject - referenceLockedCids lock was "
                                        + "interrupted while waiting to delete objects for pid: "
                                        + pid + ". InterruptedException: " + ie.getMessage();
                                logFileHashStore.error(errMsg);
                                throw new InterruptedException(errMsg);
                            }
                        }
                        logFileHashStore.debug(
                            "FileHashStore.deleteObject - Synchronizing referenceLockedCids for "
                                + "pid: " + pid + " with cid: " + cidRead);
                        referenceLockedCids.add(cidRead);
                    }

                    try {
                        // Rename pid refs file for deletion
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));

                        // Remove the pid from the cid refs file
                        Path absCidRefsPath =
                            getExpectedPath(cidRead, "refs", HashStoreIdTypes.cid.getName());
                        updateRefsFile(pid, absCidRefsPath, "remove");
                        // Add the cid reference file to deleteList if it's now empty
                        if (Files.size(absCidRefsPath) == 0) {
                            deleteList.add(
                                FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                        }
                        // Delete items
                        FileHashStoreUtility.deleteListItems(deleteList);
                        // Remove metadata files
                        deleteMetadata(pid);
                        String warnMsg = "FileHashStore.deleteObject - Object with cid: " + cidRead
                            + " does not exist, but pid and cid reference file found for pid: "
                            + pid + ". Deleted pid and cid ref files and metadata.";
                        logFileHashStore.warn(warnMsg);

                    } finally {
                        // Release lock
                        synchronized (referenceLockedCids) {
                            logFileHashStore.debug(
                                "FileHashStore.deleteObject - Releasing referenceLockedCids for "
                                    + "pid: " + pid + " with cid: " + cidRead);
                            referenceLockedCids.remove(cidRead);
                            referenceLockedCids.notify();
                        }
                    }
                } catch (PidNotFoundInCidRefsFileException pnficrfe) {
                    // `findObject` throws this exception when both the pid and cid refs file exists
                    // but the pid is not found in the cid refs file.

                    // Rename pid refs file for deletion
                    Path absPidRefsPath =
                        getExpectedPath(pid, "refs", HashStoreIdTypes.pid.getName());
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete items
                    FileHashStoreUtility.deleteListItems(deleteList);
                    // Remove metadata files
                    deleteMetadata(pid);
                    String warnMsg =
                        "FileHashStore.deleteObject - Pid not found in expected cid refs file for"
                            + " pid: " + pid + ". Deleted orphan pid refs file and metadata.";
                    logFileHashStore.warn(warnMsg);
                }
            } finally {
                // Release lock on the pid
                synchronized (objectLockedIds) {
                    logFileHashStore.debug(
                        "FileHashStore.deleteObject - Releasing objectLockedIds for pid: " + pid);
                    objectLockedIds.remove(pid);
                    objectLockedIds.notify();
                }
            }
        }
    }


    @Override
    public void deleteObject(String pid) throws IllegalArgumentException, IOException,
        NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.deleteObject - Called to delete all associated docs for pid: " + pid
        );
        deleteObject(HashStoreIdTypes.pid.getName(), pid);
        logFileHashStore.info(
            "FileHashStore.deleteObject - Object, references and metadata deleted for: " + pid
        );
    }

    @Override
    public void deleteMetadata(String pid, String formatId) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.deleteMetadata - Called to delete metadata for pid: " + pid + " with "
                + "formatId: " + formatId
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.ensureNotNull(formatId, "formatId", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyString(formatId, "formatId", "deleteMetadata");

        String metadataDocId = FileHashStoreUtility.getPidHexDigest(pid + formatId,
                                                                    OBJECT_STORE_ALGORITHM);
        synchronized (metadataLockedIds) {
            while (metadataLockedIds.contains(metadataDocId)) {
                try {
                    metadataLockedIds.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.deleteMetadata - Metadata lock was interrupted while"
                            + " deleting metadata for: " + pid + " and formatId: " + formatId
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.deleteMetadata - Synchronizing metadataLockedIds for pid: " + pid
            );
            metadataLockedIds.add(metadataDocId);
        }

        try {
            // Get permanent address of the metadata document
            Path metadataDocPath = getExpectedPath(pid, "metadata", formatId);

            if (!Files.exists(metadataDocPath)) {
                String errMsg = "FileHashStore.deleteMetadata - File does not exist for pid: " + pid
                    + " with metadata address: " + metadataDocPath;
                logFileHashStore.warn(errMsg);

            } else {
                // Proceed to delete
                Files.delete(metadataDocPath);
                logFileHashStore.info(
                    "FileHashStore.deleteMetadata - File deleted for: " + pid
                        + " with metadata address: " + metadataDocPath
                );
            }
        } finally {
            // Release lock
            synchronized (metadataLockedIds) {
                logFileHashStore.debug(
                    "FileHashStore.deleteMetadata - Releasing metadataLockedIds for pid: " + pid
                        + " and formatId " + formatId
                );
                metadataLockedIds.remove(metadataDocId);
                metadataLockedIds.notify();
            }
        }
    }

    /**
     * Overload method for deleteMetadata with default metadata namespace
     */
    @Override
    public void deleteMetadata(String pid) throws IllegalArgumentException, IOException,
        NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.deleteMetadata - Called to delete all metadata for pid: " + pid
        );
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "deleteMetadata");

        List<Path> deleteList = new ArrayList<>();
        // Metadata directory
        String pidHexDigest = FileHashStoreUtility.getPidHexDigest(pid, OBJECT_STORE_ALGORITHM);
        String pidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, pidHexDigest
        );
        Path expectedPidMetadataDirectory = METADATA_STORE_DIRECTORY.resolve(pidRelativePath);
        // Add all metadata doc paths to a List to iterate over below
        List<Path> metadataDocPaths = FileHashStoreUtility.getFilesFromDir(
            expectedPidMetadataDirectory
        );
        for (Path metadataDoc : metadataDocPaths) {
            String metadataDocId = metadataDoc.getFileName().toString();
            synchronized (metadataLockedIds) {
                while (metadataLockedIds.contains(metadataDocId)) {
                    try {
                        metadataLockedIds.wait(TIME_OUT_MILLISEC);

                    } catch (InterruptedException ie) {
                        String errMsg =
                            "FileHashStore.deleteMetadata - Metadata lock was interrupted while"
                                + " deleting metadata doc: " + metadataDocId + " for pid: " + pid
                                + ". InterruptedException: " + ie.getMessage();
                        logFileHashStore.error(errMsg);
                        throw new InterruptedException(errMsg);
                    }
                }
                logFileHashStore.debug(
                    "FileHashStore.deleteMetadata - Synchronizing metadataLockedIds for pid: " + pid
                );
                metadataLockedIds.add(metadataDocId);
            }

            try {
                deleteList.add(FileHashStoreUtility.renamePathForDeletion(metadataDoc));
            } finally {
                // Release lock
                synchronized (metadataLockedIds) {
                    logFileHashStore.debug(
                        "FileHashStore.deleteMetadata - Releasing metadataLockedIds for pid: " + pid
                            + " and doc " + metadataDocId
                    );
                    metadataLockedIds.remove(metadataDocId);
                    metadataLockedIds.notify();
                }
            }

        }
        // Delete all items in the list
        FileHashStoreUtility.deleteListItems(deleteList);
        logFileHashStore.info(
            "FileHashStore.deleteMetadata - All related metadata deleted for: " + pid
        );
    }

    @Override
    public String getHexDigest(String pid, String algorithm) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.getHexDigest - Called to calculate hex digest for pid: " + pid
        );

        FileHashStoreUtility.ensureNotNull(pid, "pid", "getHexDigest");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "getHexDigest");
        validateAlgorithm(algorithm);

        // Find the content identifier
        if (algorithm.equals(OBJECT_STORE_ALGORITHM)) {
            return findObject(pid);

        } else {
            // Get permanent address of the pid
            Path objRealPath = getExpectedPath(pid, "object", null);

            // Check to see if object exists
            if (!Files.exists(objRealPath)) {
                String errMsg = "FileHashStore.getHexDigest - File does not exist for pid: " + pid
                    + " with object address: " + objRealPath;
                logFileHashStore.warn(errMsg);
                throw new FileNotFoundException(errMsg);
            }

            InputStream dataStream = Files.newInputStream(objRealPath);
            String mdObjectHexDigest = FileHashStoreUtility.calculateHexDigest(
                dataStream, algorithm
            );
            logFileHashStore.info(
                "FileHashStore.getHexDigest - Hex digest calculated for pid: " + pid
                    + ", with hex digest value: " + mdObjectHexDigest
            );
            return mdObjectHexDigest;
        }
    }

    // FileHashStore Core & Supporting Methods

    /**
     * Takes a given InputStream and writes it to its permanent address on disk based on the SHA-256
     * hex digest value of an authority based identifier, usually provided as a persistent
     * identifier (pid).
     *
     * If an additional algorithm is provided and supported, its respective hex digest value will be
     * included in hexDigests map. If a checksum and checksumAlgorithm is provided, FileHashStore
     * will validate the given checksum against the hex digest produced of the supplied
     * checksumAlgorithm.
     *
     * @param object              InputStream for file
     * @param pid                 Authority-based identifier
     * @param additionalAlgorithm Optional checksum value to generate in hex digests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
     * @param objSize             Expected size of object to validate after storing
     * @return 'ObjectMetadata' object that contains the file id, size, and a checksum map based on
     *         the default algorithm list.
     * @throws IOException                     I/O Error when writing file, generating checksums,
     *                                         moving file or deleting tmpFile upon duplicate found
     * @throws NoSuchAlgorithmException        When additionalAlgorithm or checksumAlgorithm is
     *                                         invalid or not found
     * @throws SecurityException               Insufficient permissions to read/access files or when
     *                                         generating/writing to a file
     * @throws FileNotFoundException           tmpFile not found during store
     * @throws PidRefsFileExistsException      If the given pid already references an object
     * @throws IllegalArgumentException        When signature values are empty (checksum, pid,
     *                                         etc.)
     * @throws NullPointerException            Arguments are null for pid or object
     * @throws AtomicMoveNotSupportedException When attempting to move files across file systems
     */
    protected ObjectMetadata putObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
        PidRefsFileExistsException, IllegalArgumentException, NullPointerException,
        AtomicMoveNotSupportedException {
        logFileHashStore.debug("FileHashStore.putObject - Called to put object for pid: " + pid);
        // If validation is desired, checksumAlgorithm and checksum must both be present
        boolean requestValidation = verifyChecksumParameters(checksum, checksumAlgorithm);
        // Validate additional algorithm if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                additionalAlgorithm, "additionalAlgorithm", "putObject"
            );
            validateAlgorithm(additionalAlgorithm);
        }
        if (objSize != -1) {
            FileHashStoreUtility.checkNotNegativeOrZero(objSize, "putObject");
        }

        // Generate tmp file and write to it
        logFileHashStore.debug("FileHashStore.putObject - Generating tmpFile");
        File tmpFile = FileHashStoreUtility.generateTmpFile("tmp", OBJECT_TMP_FILE_DIRECTORY);
        Path tmpFilePath = tmpFile.toPath();
        Map<String, String> hexDigests;
        try {
            hexDigests = writeToTmpFileAndGenerateChecksums(
                tmpFile, object, additionalAlgorithm, checksumAlgorithm
            );
        } catch (Exception ge) {
            // If the process to write to the tmpFile is interrupted for any reason,
            // we will delete the tmpFile. 
            boolean deleteStatus = tmpFile.delete();
            String errMsg =
                "FileHashStore.putObject - Unexpected Exception while storing object for: " + pid;
            if (deleteStatus) {
                errMsg = errMsg + ". Deleting temp file: " + tmpFile + ". Aborting request.";
            } else {
                errMsg = errMsg + ". Failed to delete temp file: " + tmpFile
                    + ". Aborting request.";
            }
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
        long storedObjFileSize = Files.size(Paths.get(tmpFile.toString()));

        // Validate object if checksum and checksum algorithm is passed
        validateTmpObject(
            requestValidation, checksum, checksumAlgorithm, tmpFilePath, hexDigests, objSize,
            storedObjFileSize
        );

        // Gather the elements to form the permanent address
        String objectCid = hexDigests.get(OBJECT_STORE_ALGORITHM);
        String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
        );
        Path objRealPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);

        // Confirm that the object does not yet exist, delete tmpFile if so
        if (Files.exists(objRealPath)) {
            String errMsg = "FileHashStore.putObject - File already exists for pid: " + pid
                + ". Object address: " + objRealPath + ". Deleting temporary file.";
            logFileHashStore.warn(errMsg);
            tmpFile.delete();
        } else {
            // Move object
            File permFile = objRealPath.toFile();
            move(tmpFile, permFile, "object");
            logFileHashStore.debug(
                "FileHashStore.putObject - Move object success, permanent address: " + objRealPath
            );
        }

        // Create ObjectMetadata to return with pertinent data
        return new ObjectMetadata(pid, objectCid, storedObjFileSize, hexDigests);
    }

    /**
     * If requestValidation is true, determines the integrity of an object with a given checksum &
     * algorithm against a list of hex digests. If there is a mismatch, the tmpFile will be deleted
     * and exceptions will be thrown.
     *
     * @param requestValidation Boolean to decide whether to proceed with validation
     * @param checksum          Expected checksum value of object
     * @param checksumAlgorithm Hash algorithm of checksum value
     * @param tmpFile           Path to the file that is being evaluated
     * @param hexDigests        Map of the hex digests to parse data from
     * @param objSize           Expected size of object
     * @param storedObjFileSize Actual size of object stored
     * @throws NoSuchAlgorithmException If algorithm requested to validate against is absent
     * @throws IOException              Issue with deleting tmpFile
     */
    private void validateTmpObject(
        boolean requestValidation, String checksum, String checksumAlgorithm, Path tmpFile,
        Map<String, String> hexDigests, long objSize, long storedObjFileSize
    ) throws NoSuchAlgorithmException, IOException {
        if (objSize > 0) {
            if (objSize != storedObjFileSize) {
                // Delete tmp File
                try {
                    Files.delete(tmpFile);

                } catch (Exception ge) {
                    String errMsg =
                        "FileHashStore.validateTmpObject - objSize given is not equal to the"
                            + " stored object size. ObjSize: " + objSize + ". storedObjFileSize: "
                            + storedObjFileSize + ". Failed to delete tmpFile: " + tmpFile + ". "
                            + ge.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new IOException(errMsg);
                }

                String errMsg =
                    "FileHashStore.validateTmpObject - objSize given is not equal to the"
                        + " stored object size. ObjSize: " + objSize + ". storedObjFileSize: "
                        + storedObjFileSize + ". Deleting tmpFile: " + tmpFile;
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }

        if (requestValidation) {
            logFileHashStore.info(
                "FileHashStore.validateTmpObject - Validating object, checksum arguments"
                    + " supplied and valid."
            );
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (digestFromHexDigests == null) {
                String errMsg =
                    "FileHashStore.validateTmpObject - checksum not found in hex digest map"
                        + " when validating object." + " checksumAlgorithm checked: "
                        + checksumAlgorithm;
                logFileHashStore.error(errMsg);
                throw new NoSuchAlgorithmException(errMsg);
            }

            if (!checksum.equalsIgnoreCase(digestFromHexDigests)) {
                // Delete tmp File
                try {
                    Files.delete(tmpFile);

                } catch (Exception ge) {
                    String errMsg =
                        "FileHashStore.validateTmpObject - Object cannot be validated. Checksum given"
                            + " is not equal to the calculated hex digest: " + digestFromHexDigests
                            + ". Checksum" + " provided: " + checksum
                            + ". Failed to delete tmpFile: " + tmpFile + ". " + ge.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new IOException(errMsg);
                }

                String errMsg =
                    "FileHashStore.validateTmpObject - Checksum given is not equal to the"
                        + " calculated hex digest: " + digestFromHexDigests + ". Checksum"
                        + " provided: " + checksum + ". tmpFile has been deleted: " + tmpFile;
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }
    }

    /**
     * Checks whether a given algorithm is supported based on class variable
     * supportedHashAlgorithms
     *
     * @param algorithm String value (ex. SHA-256)
     * @return True if an algorithm is supported
     * @throws NullPointerException     Algorithm cannot be null
     * @throws IllegalArgumentException Algorithm cannot be empty
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    protected boolean validateAlgorithm(String algorithm) throws NullPointerException,
        IllegalArgumentException, NoSuchAlgorithmException {
        FileHashStoreUtility.ensureNotNull(algorithm, "algorithm", "putObject");
        FileHashStoreUtility.checkForEmptyString(algorithm, "algorithm", "validateAlgorithm");

        boolean algorithmSupported = Arrays.asList(SUPPORTED_HASH_ALGORITHMS).contains(algorithm);
        if (!algorithmSupported) {
            String errMsg = "FileHashStore - validateAlgorithm: Algorithm not supported: "
                + algorithm + ". Supported algorithms: " + Arrays.toString(
                    SUPPORTED_HASH_ALGORITHMS
                );
            logFileHashStore.error(errMsg);
            throw new NoSuchAlgorithmException(errMsg);
        }

        return true;
    }

    /**
     * Checks whether the algorithm supplied is included in the DefaultHashAlgorithms
     * 
     * @param algorithm Algorithm to check
     * @return True if it's included
     */
    private boolean isDefaultAlgorithm(String algorithm) {
        boolean isDefaultAlgorithm = false;
        for (DefaultHashAlgorithms defAlgo : DefaultHashAlgorithms.values()) {
            if (algorithm.equals(defAlgo.getName())) {
                isDefaultAlgorithm = true;
                break;
            }
        }
        return isDefaultAlgorithm;
    }

    /**
     * Determines whether an object will be verified with a given checksum and checksumAlgorithm
     *
     * @param checksum          Value of checksum
     * @param checksumAlgorithm Ex. "SHA-256"
     * @return True if validation is required
     * @throws NoSuchAlgorithmException If checksumAlgorithm supplied is not supported
     */
    protected boolean verifyChecksumParameters(String checksum, String checksumAlgorithm)
        throws NoSuchAlgorithmException {
        // First ensure algorithm is compatible and values are valid if they aren't null
        if (checksumAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "checksumAlgorithm", "putObject"
            );
            validateAlgorithm(checksumAlgorithm);
        }
        if (checksum != null) {
            FileHashStoreUtility.checkForEmptyString(checksum, "checksum", "putObject");
        }
        // If checksum is supplied, checksumAlgorithm cannot be empty
        if (checksum != null && !checksum.trim().isEmpty()) {
            FileHashStoreUtility.ensureNotNull(
                checksumAlgorithm, "checksumAlgorithm", "verifyChecksumParameters"
            );
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "algorithm", "verifyChecksumParameters"
            );
        }
        // Ensure algorithm is supported, not null and not empty
        boolean requestValidation = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            requestValidation = validateAlgorithm(checksumAlgorithm);
            // Ensure checksum is not null or empty if checksumAlgorithm is supplied in
            if (requestValidation) {
                FileHashStoreUtility.ensureNotNull(
                    checksum, "checksum", "verifyChecksumParameters"
                );
                FileHashStoreUtility.checkForEmptyString(
                    checksum, "checksum", "verifyChecksumParameters"
                );
            }
        }
        return requestValidation;
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap consisting of
     * algorithms and their respective hex digests. If an additional algorithm is supplied and
     * supported, it and its checksum value will be included in the hex digests map.
     *
     * Default algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
     *
     * @param tmpFile             file to write input stream data into
     * @param dataStream          input stream of data to store
     * @param additionalAlgorithm additional algorithm to include in hex digest map
     * @param checksumAlgorithm   checksum algorithm to calculate hex digest for to verifying
     *                            object
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException Unable to generate new instance of supplied algorithm
     * @throws IOException              Issue with writing file from InputStream
     * @throws SecurityException        Unable to write to tmpFile
     * @throws FileNotFoundException    tmpFile cannot be found
     */
    protected Map<String, String> writeToTmpFileAndGenerateChecksums(
        File tmpFile, InputStream dataStream, String additionalAlgorithm, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, FileNotFoundException, SecurityException {
        // Determine whether to calculate additional or checksum algorithms
        boolean generateAddAlgo = false;
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                additionalAlgorithm, "additionalAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(additionalAlgorithm);
            generateAddAlgo = !isDefaultAlgorithm(additionalAlgorithm);
        }
        boolean generateCsAlgo = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "checksumAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(checksumAlgorithm);
            generateCsAlgo = !isDefaultAlgorithm(checksumAlgorithm);
        }

        FileOutputStream os = new FileOutputStream(tmpFile);
        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.getName());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.getName());
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.getName());
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.getName());
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.getName());
        MessageDigest additionalAlgo = null;
        MessageDigest checksumAlgo = null;
        if (generateAddAlgo) {
            logFileHashStore.debug(
                "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding additional algorithm"
                    + " to hex digest map, algorithm: " + additionalAlgorithm
            );
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (generateCsAlgo) {
            logFileHashStore.debug(
                "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding checksum algorithm"
                    + " to hex digest map, algorithm: " + checksumAlgorithm
            );
            checksumAlgo = MessageDigest.getInstance(checksumAlgorithm);
        }

        // Calculate hex digests
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
                if (generateAddAlgo) {
                    additionalAlgo.update(buffer, 0, bytesRead);
                }
                if (generateCsAlgo) {
                    checksumAlgo.update(buffer, 0, bytesRead);
                }
            }

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.writeToTmpFileAndGenerateChecksums - Unexpected Exception ~ " + ioe
                    .getClass().getName() + ": " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } finally {
            dataStream.close();
            os.flush();
            os.close();
        }

        // Create map of hash algorithms and corresponding hex digests
        Map<String, String> hexDigests = new HashMap<>();
        String md5Digest = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put(DefaultHashAlgorithms.MD5.getName(), md5Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_1.getName(), sha1Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_256.getName(), sha256Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_384.getName(), sha384Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_512.getName(), sha512Digest);
        if (generateAddAlgo) {
            String extraAlgoDigest = DatatypeConverter.printHexBinary(additionalAlgo.digest())
                .toLowerCase();
            hexDigests.put(additionalAlgorithm, extraAlgoDigest);
        }
        if (generateCsAlgo) {
            String extraChecksumDigest = DatatypeConverter.printHexBinary(checksumAlgo.digest())
                .toLowerCase();
            hexDigests.put(checksumAlgorithm, extraChecksumDigest);
        }
        logFileHashStore.debug(
            "FileHashStore.writeToTmpFileAndGenerateChecksums - Object has been written to"
                + " tmpFile: " + tmpFile.getName() + ". To be moved to: " + sha256Digest
        );

        return hexDigests;
    }

    /**
     * Moves an object from one location to another. When the "entity" given is "object", if the
     * target already exists, it will throw an exception to prevent the target object to be
     * overwritten. Data "object" files are stored once and only once.
     *
     * @param source File to move
     * @param target Where to move the file
     * @param entity Type of object to move
     * @throws FileAlreadyExistsException      Target file already exists
     * @throws IOException                     Unable to create parent directory
     * @throws SecurityException               Insufficient permissions to move file
     * @throws AtomicMoveNotSupportedException When ATOMIC_MOVE is not supported (usually
     *                                         encountered when moving across file systems)
     */
    protected void move(File source, File target, String entity) throws IOException,
        SecurityException, AtomicMoveNotSupportedException, FileAlreadyExistsException {
        logFileHashStore.debug(
            "FileHashStore.move - called to move entity type: " + entity + ", from source: "
                + source + ", to target: " + target
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(entity, "entity", "move");
        FileHashStoreUtility.checkForEmptyString(entity, "entity", "move");
        // Entity is only used when checking for an existence of an object
        if (entity.equals("object") && target.exists()) {
            String errMsg = "FileHashStore.move - File already exists for target: " + target;
            logFileHashStore.warn(errMsg);
            return;
        }

        File destinationDirectory = new File(target.getParent());
        // Create parent directory if it doesn't exist
        if (!destinationDirectory.exists()) {
            Path destinationDirectoryPath = destinationDirectory.toPath();

            try {
                Files.createDirectories(destinationDirectoryPath);

            } catch (FileAlreadyExistsException faee) {
                logFileHashStore.warn(
                    "FileHashStore.move - Directory already exists at: "
                        + destinationDirectoryPath + " - Skipping directory creation"
                );
            }
        }

        // Move file
        Path sourceFilePath = source.toPath();
        Path targetFilePath = target.toPath();
        try {
            Files.move(sourceFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
            logFileHashStore.debug(
                "FileHashStore.move - file moved from: " + sourceFilePath + ", to: "
                    + targetFilePath
            );

        } catch (FileAlreadyExistsException faee) {
            logFileHashStore.warn(
                "FileHashStore.move - File already exists, skipping request to move object."
                    + " Source: " + source + ". Target: " + target
            );

        } catch (AtomicMoveNotSupportedException amnse) {
            logFileHashStore.error(
                "FileHashStore.move - StandardCopyOption.ATOMIC_MOVE failed. AtomicMove is"
                    + " not supported across file systems. Source: " + source + ". Target: "
                    + target
            );
            throw amnse;

        } catch (IOException ioe) {
            logFileHashStore.error(
                "FileHashStore.move - Unable to move file. Source: " + source + ". Target: "
                    + target
            );
            throw ioe;

        }
    }

    /**
     * Attempt to delete an object based on the given content identifier (cid). If the object has
     * pids that references it and/or a cid refs file exists, the object will not be deleted.
     *
     * @param cid Content identifier
     * @throws IOException              If an issue arises during deletion of object
     * @throws NoSuchAlgorithmException Incompatible algorithm used to find relative path to cid
     * @throws InterruptedException     Issue with synchronization of cid deletion
     */
    protected void deleteObjectByCid(String cid)
        throws IOException, NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug("FileHashStore - deleteObjectByCid: called to delete cid: " + cid);
        // Get expected path of the cid refs file
        Path absCidRefsPath = getExpectedPath(cid, "refs", HashStoreIdTypes.cid.getName());
        // Get permanent address of the actual cid
        String objRelativePath =
            FileHashStoreUtility.getHierarchicalPathString(DIRECTORY_DEPTH, DIRECTORY_WIDTH, cid);
        Path expectedRealPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);

        // Minimize the amount of time the cid is locked
        synchronized (referenceLockedCids) {
            while (referenceLockedCids.contains(cid)) {
                try {
                    referenceLockedCids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.deleteObjectByCid - referenceLockedCids lock was "
                            + "interrupted while waiting to delete object with cid: " + cid
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.deleteObjectByCid - Synchronizing referenceLockedCids for cid: "
                    + cid);
            referenceLockedCids.add(cid);
        }

        try {
            if (Files.exists(absCidRefsPath)) {
                // The cid refs file exists, so the cid object cannot be deleted.
                String warnMsg = "FileHashStore - deleteObjectByCid: cid refs file still contains"
                    + " references, skipping deletion.";
                logFileHashStore.warn(warnMsg);
            } else {
                // If file exists, delete it.
                if (Files.exists(expectedRealPath)) {
                    Files.delete(expectedRealPath);
                }
                String debugMsg =
                    "FileHashStore - deleteObjectByCid: object deleted at" + expectedRealPath;
                logFileHashStore.debug(debugMsg);
            }
        } finally {
            // Release lock
            synchronized (referenceLockedCids) {
                logFileHashStore.debug(
                    "FileHashStore.deleteObjectByCid - Releasing referenceLockedCids for cid: "
                        + cid);
                referenceLockedCids.remove(cid);
                referenceLockedCids.notify();
            }
        }
    }

    /**
     * Verifies that the reference files for the given pid and cid exist and contain
     * the expected values.
     * 
     * @param pid            Authority-based or persistent identifier
     * @param cid            Content identifier
     * @param absPidRefsPath Path to where the pid refs file exists
     * @param absCidRefsPath Path to where the cid refs file exists
     * @throws FileNotFoundException Any refs files are missing
     * @throws IOException           Unable to read any of the refs files or if the refs content
     *                               is not what is expected
     */
    protected void verifyHashStoreRefsFiles(
        String pid, String cid, Path absPidRefsPath, Path absCidRefsPath
    ) throws FileNotFoundException, IOException {
        // First confirm that the files were created
        if (!Files.exists(absCidRefsPath)) {
            String errMsg = "FileHashStore.verifyHashStoreRefsFiles - cid refs file is missing: "
                + absCidRefsPath + " for pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
        if (!Files.exists(absPidRefsPath)) {
            String errMsg = "FileHashStore.verifyHashStoreRefsFiles - pid refs file is missing: "
                + absPidRefsPath + " for cid: " + cid;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
        // Now verify the content
        try {
            String cidRead = new String(Files.readAllBytes(absPidRefsPath));
            if (!cidRead.equals(cid)) {
                String errMsg = "FileHashStore.verifyHashStoreRefsFiles - Unexpected cid: "
                    + cidRead + " found in pid refs file: " + absPidRefsPath + ". Expected cid: "
                    + cid;
                logFileHashStore.error(errMsg);
                throw new IOException(errMsg);
            }
            boolean pidFoundInCidRefFiles = isStringInRefsFile(pid, absCidRefsPath);
            if (!pidFoundInCidRefFiles) {
                String errMsg = "FileHashStore.verifyHashStoreRefsFiles - Missing expected pid: "
                    + pid + " in cid refs file: " + absCidRefsPath;
                logFileHashStore.error(errMsg);
                throw new IOException(errMsg);
            }
        } catch (IOException ioe) {
            String errMsg = "FileHashStore.verifyHashStoreRefsFiles - " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }


    /**
     * Writes the given ref into a temporary file. The client must explicitly move this file to
     * where it belongs otherwise it will be removed during garbage collection.
     *
     * @param ref     Authority-based or persistent identifier to write
     * @param refType Type of reference 'pid', 'cid' or 'sysmeta'
     * @throws IOException Failure to write refs file
     * @return File object with single reference
     */
    protected File writeRefsFile(String ref, String refType) throws IOException {
        File cidRefsTmpFile = FileHashStoreUtility.generateTmpFile("tmp", REFS_TMP_FILE_DIRECTORY);
        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(cidRefsTmpFile.toPath()), StandardCharsets.UTF_8
            )
        )) {
            writer.write(ref);
            writer.close();

            logFileHashStore.debug(
                "FileHashStore.writeRefsFile - " + refType + " refs file written for: " + ref
            );
            return cidRefsTmpFile;

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.writeRefsFile - Unable to write refs file for ref: "
                + refType + " IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Checks a given refs file for a ref. This is case-sensitive.
     * 
     * @param ref         Authority-based or persistent identifier to search
     * @param absRefsPath Path to the refs file to check
     * @return True if cid is found, false otherwise
     * @throws IOException If unable to read the cid refs file.
     */
    protected boolean isStringInRefsFile(String ref, Path absRefsPath) throws IOException {
        List<String> lines = Files.readAllLines(absRefsPath);
        boolean refFoundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(ref)) {
                refFoundInCidRefFiles = true;
                break;
            }
        }
        return refFoundInCidRefFiles;
    }

    /**
     * Adds or removes a ref value from a refs file given an 'updateType'
     * 
     * @param ref         Authority-based or persistent identifier
     * @param absRefsPath Path to the refs file to update
     * @param updateType  "add" or "remove"
     * @throws IOException Issue with updating or accessing a refs file
     */
    protected void updateRefsFile(String ref, Path absRefsPath, String updateType)
        throws IOException {
        // This update process is atomic, so we first write the updated content
        // into a temporary file before overwriting it.
        File tmpFile = FileHashStoreUtility.generateTmpFile("tmp", REFS_TMP_FILE_DIRECTORY);
        Path tmpFilePath = tmpFile.toPath();

        try {
            // Obtain a lock on the file before updating it
            try (FileChannel channel = FileChannel.open(
                absRefsPath, StandardOpenOption.READ, StandardOpenOption.WRITE
            ); FileLock ignored = channel.lock()) {
                List<String> lines = new ArrayList<>(Files.readAllLines(absRefsPath));

                if (updateType.equals("add")) {
                    lines.add(ref);
                    Files.write(tmpFilePath, lines, StandardOpenOption.WRITE);
                    move(tmpFile, absRefsPath.toFile(), "refs");
                    logFileHashStore.debug(
                        "FileHashStore.updateRefsFile - Ref: " + ref
                            + " has been added to refs file: " + absRefsPath
                    );
                }

                if (updateType.equals("remove")) {
                    lines.remove(ref);
                    Files.write(tmpFilePath, lines, StandardOpenOption.WRITE);
                    move(tmpFile, absRefsPath.toFile(), "refs");
                    logFileHashStore.debug(
                        "FileHashStore.updateRefsFile - Ref: " + ref
                            + " has been removed from refs file: " + absRefsPath
                    );
                }
            }
            // The lock is automatically released when the try block exits
        } catch (IOException ioe) {
            String errMsg = "FileHashStore.updateRefsFile - " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Deletes a references file at the given path
     * 
     * @param absRefsPath Path to the refs file to delete
     * @throws IOException Unable to delete object or open pid refs file
     */
    protected void deleteRefsFile(Path absRefsPath) throws IOException {
        // Check to see if pid refs file exists
        if (!Files.exists(absRefsPath)) {
            String errMsg = "FileHashStore.deleteRefsFile - Refs file does not exist at: "
                + absRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);

        } else {
            // Proceed to delete
            Files.delete(absRefsPath);
            logFileHashStore.debug(
                "FileHashStore.deleteRefsFile - Refs file deleted at: " + absRefsPath
            );
        }
    }

    /**
     * Takes a given input stream and writes it to its permanent address on disk based on the
     * SHA-256 hex digest of the given pid + formatId. If no formatId is supplied, it will use the
     * default store namespace as defined by `hashstore.yaml`
     *
     * @param metadata InputStream to metadata
     * @param pid      Authority-based identifier
     * @param formatId Metadata formatId or namespace
     * @return Metadata content identifier
     * @throws NoSuchAlgorithmException When the algorithm used to calculate permanent address is
     *                                  not supported
     * @throws IOException              I/O error when writing to tmp file
     */
    protected String putMetadata(InputStream metadata, String pid, String formatId)
        throws NoSuchAlgorithmException, IOException {
        logFileHashStore.debug(
            "FileHashStore.putMetadata - Called to put metadata for pid: " + pid
                + " , with metadata namespace: " + formatId
        );

        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(metadata, "metadata", "putMetadata");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "putMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "putMetadata");

        // Determine metadata namespace
        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = DEFAULT_METADATA_NAMESPACE;
        } else {
            FileHashStoreUtility.checkForEmptyString(formatId, "formatId", "putMetadata");
            checkedFormatId = formatId;
        }

        // Get permanent address for the given metadata document
        // All metadata documents for a pid are stored in a directory that is formed
        // by using the hash of the 'pid', with the file name being the hash of the 'pid+formatId'
        Path pathToStoredMetadata = getExpectedPath(pid, "metadata", checkedFormatId);

        // Store metadata to tmpMetadataFile
        File tmpMetadataFile = FileHashStoreUtility.generateTmpFile(
            "tmp", METADATA_TMP_FILE_DIRECTORY
        );
        boolean tmpMetadataWritten = writeToTmpMetadataFile(tmpMetadataFile, metadata);
        if (tmpMetadataWritten) {
            logFileHashStore.debug(
                "FileHashStore.putMetadata - tmp metadata file has been written, moving to"
                    + " permanent location: " + pathToStoredMetadata
            );
            File permMetadataFile = pathToStoredMetadata.toFile();
            move(tmpMetadataFile, permMetadataFile, "metadata");
        }
        logFileHashStore.debug(
            "FileHashStore.putMetadata - Move metadata success, permanent address: "
                + pathToStoredMetadata
        );
        return pathToStoredMetadata.toString();
    }

    /**
     * Write the supplied metadata content into the given tmpFile
     *
     * @param tmpFile        File to write into
     * @param metadataStream Stream of metadata content
     * @return True if file is written successfully
     * @throws IOException           When an I/O error occurs
     * @throws FileNotFoundException When given file to write into is not found
     */
    protected boolean writeToTmpMetadataFile(File tmpFile, InputStream metadataStream)
        throws IOException, FileNotFoundException {
        FileOutputStream os = new FileOutputStream(tmpFile);

        try {
            // Write metadata content
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = metadataStream.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
            }
            return true;

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.writeToTmpMetadataFile - Unexpected IOException encountered: " + ioe
                    .getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } finally {
            os.flush();
            os.close();
        }
    }

    /**
     * Get the absolute path of a HashStore object, metadata or refs file
     *
     * @param abId     Authority-based, persistent or content identifier
     * @param entity   "object" or "metadata"
     * @param formatId Metadata namespace or reference type (pid/cid)
     * @return Actual path to object
     * @throws IllegalArgumentException If entity is not object or metadata
     * @throws NoSuchAlgorithmException If store algorithm is not supported
     * @throws IOException              If unable to retrieve cid
     */
    protected Path getExpectedPath(String abId, String entity, String formatId)
        throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
        Path realPath;
        String hashId = FileHashStoreUtility.getPidHexDigest(abId, OBJECT_STORE_ALGORITHM);
        if (entity.equalsIgnoreCase("object")) {
            // `hashId` here is the address of the pid refs file, and contains the cid
            String pidRefsFileRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashId
            );
            Path pathToPidRefsFile = REFS_PID_FILE_DIRECTORY.resolve(pidRefsFileRelativePath);
            // Attempt to retrieve the cid
            String objectCid;
            if (!Files.exists(pathToPidRefsFile)) {
                String errMsg =
                    "FileHashStore.getExpectedPath - Pid Refs file does not exist for pid: " + abId
                        + " with object address: " + pathToPidRefsFile + ". Cannot retrieve cid.";
                logFileHashStore.warn(errMsg);
                throw new FileNotFoundException(errMsg);
            } else {
                objectCid = new String(Files.readAllBytes(pathToPidRefsFile));
            }
            // If cid is found, return the expected real path to object
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
            );
            realPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
        } else if (entity.equalsIgnoreCase("metadata")) {
            // Get the pid metadata directory (the sharded path of the hashId)
            String pidMetadataDirRelPath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashId
            );
            // The file name for the metadata document is the hash of the supplied 'pid + 'formatId'
            String metadataDocHash =
                FileHashStoreUtility.getPidHexDigest(abId + formatId, OBJECT_STORE_ALGORITHM);
            realPath = METADATA_STORE_DIRECTORY.resolve(pidMetadataDirRelPath).resolve(
                metadataDocHash
            );

        } else if (entity.equalsIgnoreCase("refs")) {
            if (formatId.equalsIgnoreCase(HashStoreIdTypes.pid.getName())) {
                // `hashId` here is the pid refs file string to split
                String pidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashId
                );
                realPath = REFS_PID_FILE_DIRECTORY.resolve(pidRelativePath);
            } else if (formatId.equalsIgnoreCase(HashStoreIdTypes.cid.getName())) {
                // If refs type is 'cid', use the abId directly provided
                String cidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, abId
                );
                realPath = REFS_CID_FILE_DIRECTORY.resolve(cidRelativePath);
            } else {
                String errMsg =
                    "FileHashStore.getExpectedPath - formatId must be 'pid' or 'cid' when entity is 'refs'";
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }

        } else {
            throw new IllegalArgumentException(
                "FileHashStore.getExpectedPath - entity must be 'object', 'metadata' or 'refs'"
            );
        }
        return realPath;
    }
}

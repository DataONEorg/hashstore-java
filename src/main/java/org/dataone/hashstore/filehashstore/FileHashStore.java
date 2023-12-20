package org.dataone.hashstore.filehashstore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
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
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.HashStore;
import org.dataone.hashstore.exceptions.PidExistsInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidObjectExistsException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;

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
        REFS_PID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("pid");
        REFS_CID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("cid");

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
     */
    protected void verifyHashStoreProperties(
        Path storePath, int storeDepth, int storeWidth, String storeAlgorithm,
        String storeMetadataNamespace
    ) throws NoSuchAlgorithmException, IOException {
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
            checkConfigurationEquality("store depth", storeDepth, existingStoreDepth);
            checkConfigurationEquality("store width", storeWidth, existingStoreWidth);
            checkConfigurationEquality("store algorithm", storeAlgorithm, existingStoreAlgorithm);
            checkConfigurationEquality(
                "store metadata namespace", storeMetadataNamespace, existingStoreMetadataNs
            );

        } else {
            // Check if HashStore exists at the given store path (and is missing config)
            logFileHashStore.debug(
                "FileHashStore - 'hashstore.yaml' not found, check store path for"
                    + " objects and directories."
            );

            if (Files.isDirectory(storePath)) {
                if (!FileHashStoreUtility.isDirectoryEmpty(storePath)) {
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
            writer.close();

        } catch (IOException ioe) {
            logFileHashStore.fatal(
                "FileHashStore.writeHashStoreYaml() - Unable to write 'hashstore.yaml'."
                    + " IOException: " + ioe.getMessage()
            );
            throw ioe;
        }
    }

    /**
     * Checks the equality of a supplied value with an existing value for a specific configuration
     * property.
     *
     * @param propertyName  The name of the config property being checked
     * @param suppliedValue The value supplied for the config property
     * @param existingValue The existing value of the config property
     * @throws IllegalArgumentException If the supplied value is not equal to the existing value
     */
    protected void checkConfigurationEquality(
        String propertyName, Object suppliedValue, Object existingValue
    ) {
        if (!Objects.equals(suppliedValue, existingValue)) {
            String errMsg = "FileHashStore.checkConfigurationEquality() - Supplied " + propertyName
                + ": " + suppliedValue + " does not match the existing configuration value: "
                + existingValue;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
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
    ) throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException,
        InterruptedException {
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
    ) throws NoSuchAlgorithmException, PidObjectExistsException, IOException, RuntimeException,
        InterruptedException {
        // Lock pid for thread safety, transaction control and atomic writing
        // A pid can only be stored once and only once, subsequent calls will
        // be accepted but will be rejected if pid hash object exists
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
            String cid = objInfo.getId();
            tagObject(pid, cid);
            logFileHashStore.info(
                "FileHashStore.syncPutObject - Object stored for pid: " + pid
                    + ". Permanent address: " + getRealPath(pid, "object", null)
            );
            return objInfo;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". NoSuchAlgorithmException: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } catch (PidObjectExistsException poee) {
            String errMsg = "FileHashStore.syncPutObject - Unable to store object for pid: " + pid
                + ". PidObjectExistsException: " + poee.getMessage();
            logFileHashStore.error(errMsg);
            throw poee;

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
                objectLockedIds.notifyAll();
            }
        }
    }

    /**
     * Overload method for storeObject with just an InputStream
     */
    @Override
    public ObjectMetadata storeObject(InputStream object) throws NoSuchAlgorithmException,
        IOException, PidObjectExistsException, RuntimeException {
        // 'putObject' is called directly to bypass the pid synchronization implemented to
        // efficiently handle duplicate object store requests. Since there is no pid, calling
        // 'storeObject' would unintentionally create a bottleneck for all requests without a
        // pid (they would be executed sequentially). This scenario occurs when metadata about
        // the object (ex. form data including the pid, checksum, checksum algorithm, etc.) is
        // unavailable.
        //
        // Note: This method does not tag the object to make it discoverable, so the client must
        // call 'tagObject' and 'verifyObject' separately to ensure that the object stored
        // is discoverable and is what is expected.
        return putObject(object, "HashStoreNoPid", null, null, null, -1);
    }

    /**
     * Overload method for storeObject with an additionalAlgorithm
     */
    @Override
    public ObjectMetadata storeObject(InputStream object, String pid, String additionalAlgorithm)
        throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.ensureNotNull(
            additionalAlgorithm, "additionalAlgorithm", "storeObject"
        );

        return storeObject(object, pid, additionalAlgorithm, null, null, -1);
    }

    /**
     * Overload method for storeObject with just a checksum and checksumAlgorithm
     */
    @Override
    public ObjectMetadata storeObject(
        InputStream object, String pid, String checksum, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "storeObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "storeObject");

        return storeObject(object, pid, null, checksum, checksumAlgorithm, -1);
    }

    /**
     * Overload method for storeObject with size of object to validate
     */
    @Override
    public ObjectMetadata storeObject(InputStream object, String pid, long objSize)
        throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException,
        InterruptedException {
        FileHashStoreUtility.checkNotNegativeOrZero(objSize, "storeObject");

        return storeObject(object, pid, null, null, null, objSize);
    }

    @Override
    public void verifyObject(
        ObjectMetadata objectInfo, String checksum, String checksumAlgorithm, long objSize
    ) throws IOException, NoSuchAlgorithmException, IllegalArgumentException {
        FileHashStoreUtility.ensureNotNull(objectInfo, "objectInfo", "verifyObject");
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "verifyObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "verifyObject");

        Map<String, String> hexDigests = objectInfo.getHexDigests();
        long objInfoRetrievedSize = objectInfo.getSize();
        String objId = objectInfo.getId();
        // Object is not tagged at this stage, so we must manually form the permanent address of the file
        String cidShardString = getHierarchicalPathString(DIRECTORY_DEPTH, DIRECTORY_WIDTH, objId);
        Path objAbsPath = OBJECT_STORE_DIRECTORY.resolve(cidShardString);

        validateTmpObject(
            true, checksum, checksumAlgorithm, objAbsPath, hexDigests, objSize, objInfoRetrievedSize
        );
    }

    @Override
    public void tagObject(String pid, String cid) throws IOException, PidRefsFileExistsException,
        NoSuchAlgorithmException, FileNotFoundException, PidExistsInCidRefsFileException,
        InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.tagObject - Called to tag cid (" + cid + ") with pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "tagObject");
        FileHashStoreUtility.ensureNotNull(cid, "cid", "tagObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "tagObject");
        FileHashStoreUtility.checkForEmptyString(cid, "cid", "tagObject");

        synchronized (referenceLockedCids) {
            while (referenceLockedCids.contains(cid)) {
                try {
                    referenceLockedCids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.tagObject - referenceLockedCids lock was interrupted while"
                            + " waiting to tag pid: " + pid + " and cid: " + cid
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.tagObject - Synchronizing referenceLockedCids for cid: " + cid
            );
            referenceLockedCids.add(cid);
        }

        try {
            Path absPidRefsPath = getRealPath(pid, "refs", "pid");
            Path absCidRefsPath = getRealPath(cid, "refs", "cid");

            // Check that pid refs file doesn't exist yet
            if (Files.exists(absPidRefsPath)) {
                String errMsg = "FileHashStore.tagObject - pid refs file already exists for pid: "
                    + pid + ". A pid can only reference one cid.";
                logFileHashStore.error(errMsg);
                throw new PidRefsFileExistsException(errMsg);

            } else if (Files.exists(absCidRefsPath)) {
                // Ensure that the pid is not already found in the file
                boolean pidFoundInCidRefFiles = isPidInCidRefsFile(pid, absCidRefsPath);
                if (pidFoundInCidRefFiles) {
                    String errMsg = "FileHashStore.tagObject - cid refs file already contains pid: "
                        + pid + ". Refs file not created for both the given pid. Cid refs file ("
                        + absCidRefsPath + ") has not been updated.";
                    logFileHashStore.error(errMsg);
                    throw new PidExistsInCidRefsFileException(errMsg);
                }

                // Write pid refs file to tmp file
                File pidRefsTmpFile = writePidRefsFile(cid);
                File absPathPidRefsFile = absPidRefsPath.toFile();
                move(pidRefsTmpFile, absPathPidRefsFile, "refs");
                // Now update cid refs file
                updateCidRefsFiles(pid, absCidRefsPath);
                // Verify tagging process, this throws exceptions if there's an issue
                verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);

                logFileHashStore.info(
                    "FileHashStore.tagObject - Object with cid: " + cid
                        + " has been updated successfully with pid: " + pid
                );

            } else {
                // Get pid and cid refs files 
                File pidRefsTmpFile = writePidRefsFile(cid);
                File cidRefsTmpFile = writeCidRefsFile(pid);
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
            }

        } finally {
            // Release lock
            synchronized (referenceLockedCids) {
                logFileHashStore.debug(
                    "FileHashStore.tagObject - Releasing referenceLockedCids for cid: " + cid
                );
                referenceLockedCids.remove(cid);
                referenceLockedCids.notifyAll();
            }
        }
    }

    @Override
    public String findObject(String pid) throws NoSuchAlgorithmException, IOException {
        logFileHashStore.debug("FileHashStore.findObject - Called to find object for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(pid, "pid", "findObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "findObject");

        // Get path of the pid references file
        Path absPidRefsPath = getRealPath(pid, "refs", "pid");

        if (Files.exists(absPidRefsPath)) {
            String cidFromPidRefsFile = new String(Files.readAllBytes(absPidRefsPath));
            logFileHashStore.info(
                "FileHashStore.findObject - Cid (" + cidFromPidRefsFile + ") found for pid:" + pid
            );
            return cidFromPidRefsFile;

        } else {
            String errMsg = "FileHashStore.findObject - Unable to find cid for pid: " + pid
                + ". Pid refs file does not exist at: " + absPidRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
    }

    @Override
    public String storeMetadata(InputStream metadata, String pid, String formatId)
        throws IOException, FileNotFoundException, IllegalArgumentException, InterruptedException,
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
        synchronized (metadataLockedIds) {
            while (metadataLockedIds.contains(pidFormatId)) {
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
            metadataLockedIds.add(pidFormatId);
        }

        try {
            logFileHashStore.debug(
                "FileHashStore.storeMetadata - .putMetadata() request for pid: " + pid
                    + ". formatId: " + checkedFormatId
            );
            // Store metadata
            String metadataCid = putMetadata(metadata, pid, checkedFormatId);
            logFileHashStore.info(
                "FileHashStore.storeMetadata - Metadata stored for pid: " + pid
                    + ". Metadata Content Identifier (metadataCid): " + metadataCid
            );
            return metadataCid;

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
                metadataLockedIds.remove(pidFormatId);
                metadataLockedIds.notifyAll();
            }
        }
    }

    /**
     * Overload method for storeMetadata with default metadata namespace
     */
    @Override
    public String storeMetadata(InputStream metadata, String pid) throws IOException,
        IllegalArgumentException, InterruptedException, NoSuchAlgorithmException {
        return storeMetadata(metadata, pid, DEFAULT_METADATA_NAMESPACE);
    }

    @Override
    public InputStream retrieveObject(String pid) throws IllegalArgumentException,
        NoSuchAlgorithmException, FileNotFoundException, IOException {
        logFileHashStore.debug(
            "FileHashStore.retrieveObject - Called to retrieve object for pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "retrieveObject");

        // Get permanent address of the pid by calculating its sha-256 hex digest
        Path objRealPath = getRealPath(pid, "object", null);

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
        Path metadataCidPath = getRealPath(pid, "metadata", formatId);

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
        Path metadataCidPath = getRealPath(pid, "metadata", DEFAULT_METADATA_NAMESPACE);

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
    public void deleteObject(String pid) throws IllegalArgumentException, FileNotFoundException,
        IOException, NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "FileHashStore.deleteObject - Called to delete object for pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteObject");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "deleteObject");

        String cid = findObject(pid);

        synchronized (referenceLockedCids) {
            while (referenceLockedCids.contains(cid)) {
                try {
                    referenceLockedCids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "FileHashStore.deleteObject - referenceLockedCids lock was interrupted while"
                            + " waiting to delete object with cid: " + cid
                            + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "FileHashStore.deleteObject - Synchronizing referenceLockedCids for cid: " + cid
            );
            referenceLockedCids.add(cid);
        }

        try {
            // Get permanent address of the pid by calculating its sha-256 hex digest
            Path objRealPath = getRealPath(pid, "object", null);

            // Check to see if object exists
            if (!Files.exists(objRealPath)) {
                String errMsg = "FileHashStore.deleteObject - File does not exist for pid: " + pid
                    + " with object address: " + objRealPath;
                logFileHashStore.warn(errMsg);
                throw new FileNotFoundException(errMsg);
            }

            // Proceed to delete
            Files.delete(objRealPath);
            // Remove pid from the cid refs file
            deleteCidRefsPid(pid, cid);
            // Delete pid reference file
            deletePidRefsFile(pid);

            logFileHashStore.info(
                "FileHashStore.deleteObject - File and references deleted for: " + pid
                    + " with object address: " + objRealPath
            );

        } finally {
            // Release lock
            synchronized (referenceLockedCids) {
                logFileHashStore.debug(
                    "FileHashStore.deleteObject - Releasing referenceLockedCids for cid: " + cid
                );
                referenceLockedCids.remove(cid);
                referenceLockedCids.notifyAll();
            }
        }

    }

    @Override
    public void deleteMetadata(String pid, String formatId) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug(
            "FileHashStore.deleteMetadata - Called to delete metadata for pid: " + pid
        );
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.ensureNotNull(formatId, "formatId", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyString(formatId, "formatId", "deleteMetadata");

        // Get permanent address of the pid by calculating its sha-256 hex digest
        Path metadataCidPath = getRealPath(pid, "metadata", formatId);

        // Check to see if object exists
        if (!Files.exists(metadataCidPath)) {
            String errMsg = "FileHashStore.deleteMetadata - File does not exist for pid: " + pid
                + " with metadata address: " + metadataCidPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // Proceed to delete
        Files.delete(metadataCidPath);
        logFileHashStore.info(
            "FileHashStore.deleteMetadata - File deleted for: " + pid + " with metadata address: "
                + metadataCidPath
        );
    }

    /**
     * Overload method for deleteMetadata with default metadata namespace
     */
    public void deleteMetadata(String pid) throws IllegalArgumentException, FileNotFoundException,
        IOException, NoSuchAlgorithmException {
        deleteMetadata(pid, DEFAULT_METADATA_NAMESPACE);
    }

    @Override
    public String getHexDigest(String pid, String algorithm) throws NoSuchAlgorithmException,
        FileNotFoundException, IOException {
        logFileHashStore.debug(
            "FileHashStore.getHexDigest - Called to calculate hex digest for pid: " + pid
        );

        FileHashStoreUtility.ensureNotNull(pid, "pid", "getHexDigest");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "getHexDigest");
        validateAlgorithm(algorithm);

        // Find the content identifier
        if (algorithm.equals(OBJECT_STORE_ALGORITHM)) {
            String cid = findObject(pid);
            return cid;

        } else {
            // Get permanent address of the pid
            Path objRealPath = getRealPath(pid, "object", null);

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
     * @throws PidObjectExistsException        Duplicate object in store exists
     * @throws IllegalArgumentException        When signature values are empty (checksum, pid,
     *                                         etc.)
     * @throws NullPointerException            Arguments are null for pid or object
     * @throws AtomicMoveNotSupportedException When attempting to move files across file systems
     */
    protected ObjectMetadata putObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
        PidObjectExistsException, IllegalArgumentException, NullPointerException,
        AtomicMoveNotSupportedException {
        logFileHashStore.debug("FileHashStore.putObject - Called to put object for pid: " + pid);

        // Validate algorithms if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                additionalAlgorithm, "additionalAlgorithm", "putObject"
            );
            validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "checksumAlgorithm", "putObject"
            );
            validateAlgorithm(checksumAlgorithm);
        }
        if (objSize != -1) {
            FileHashStoreUtility.checkNotNegativeOrZero(objSize, "putObject");
        }

        // If validation is desired, checksumAlgorithm and checksum must both be present
        boolean requestValidation = verifyChecksumParameters(checksum, checksumAlgorithm);

        // Generate tmp file and write to it
        logFileHashStore.debug("FileHashStore.putObject - Generating tmpFile");
        File tmpFile = generateTmpFile("tmp", OBJECT_TMP_FILE_DIRECTORY);
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
        String objShardString = getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
        );
        Path objRealPath = OBJECT_STORE_DIRECTORY.resolve(objShardString);

        // Confirm that the object does not yet exist, delete tmpFile if so
        if (Files.exists(objRealPath)) {
            String errMsg = "FileHashStore.putObject - File already exists for pid: " + pid
                + ". Object address: " + objRealPath + ". Aborting request.";
            logFileHashStore.warn(errMsg);
            tmpFile.delete();
            throw new PidObjectExistsException(errMsg);
        } else {
            // Move object
            File permFile = objRealPath.toFile();
            move(tmpFile, permFile, "object");
            logFileHashStore.debug(
                "FileHashStore.putObject - Move object success, permanent address: " + objRealPath
            );
        }

        // Create ObjectMetadata to return with pertinent data
        return new ObjectMetadata(objectCid, storedObjFileSize, hexDigests);
    }

    /**
     * If requestValidation is true, determines the integrity of an object with a given checksum &
     * algorithm against a list of hex digests. If there is a mismatch, the tmpFile will be deleted
     * and exceptions will be thrown.
     *
     * @param requestValidation Boolean to decide whether to proceed with validation
     * @param checksum          Expected checksum value of object
     * @param checksumAlgorithm Hash algorithm of checksum value
     * @param tmpFile           tmpFile that has been written
     * @param hexDigests        Map of the hex digests available to check with
     * @throws NoSuchAlgorithmException When algorithm supplied is not supported
     * @throws IOException              When tmpFile fails to be deleted
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
                            + " stored object size. ObjSize: " + objSize + ". storedObjFileSize:"
                            + storedObjFileSize + ". Failed to delete tmpFile: " + tmpFile;
                    logFileHashStore.error(errMsg);
                    throw new IOException(errMsg);
                }

                String errMsg =
                    "FileHashStore.validateTmpObject - objSize given is not equal to the"
                        + " stored object size. ObjSize: " + objSize + ". storedObjFileSize:"
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
                            + ". Failed to delete tmpFile: " + tmpFile;
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
            String errMsg = "Algorithm not supported: " + algorithm + ". Supported algorithms: "
                + Arrays.toString(SUPPORTED_HASH_ALGORITHMS);
            logFileHashStore.error(errMsg);
            throw new NoSuchAlgorithmException(errMsg);
        }

        return true;
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
     * Given a string and supported algorithm returns the hex digest
     *
     * @param pid       authority based identifier or persistent identifier
     * @param algorithm string value (ex. SHA-256)
     * @return Hex digest of the given string in lower-case
     * @throws IllegalArgumentException String or algorithm cannot be null or empty
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    protected String getPidHexDigest(String pid, String algorithm) throws NoSuchAlgorithmException,
        IllegalArgumentException {
        FileHashStoreUtility.ensureNotNull(pid, "pid", "getPidHexDigest");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "getPidHexDigest");
        FileHashStoreUtility.ensureNotNull(algorithm, "algorithm", "getPidHexDigest");
        FileHashStoreUtility.checkForEmptyString(algorithm, "algorithm", "getPidHexDigest");
        validateAlgorithm(algorithm);

        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = pid.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        // stringDigest
        return DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
    }

    /**
     * Generates a hierarchical path by dividing a given digest into tokens of fixed width, and
     * concatenating them with '/' as the delimiter.
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
     * @return Temporary file (File) ready to write into
     * @throws IOException       Issues with generating tmpFile
     * @throws SecurityException Insufficient permissions to create tmpFile
     */
    protected File generateTmpFile(String prefix, Path directory) throws IOException,
        SecurityException {
        Random rand = new Random();
        int randomNumber = rand.nextInt(1000000);
        String newPrefix = prefix + "-" + System.currentTimeMillis() + randomNumber;

        try {
            Path newPath = Files.createTempFile(directory, newPrefix, null);
            File newFile = newPath.toFile();
            logFileHashStore.trace(
                "FileHashStore.generateTmpFile - tmpFile generated: " + newFile.getAbsolutePath()
            );
            newFile.deleteOnExit();
            return newFile;

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.generateTmpFile - Unable to generate tmpFile: " + ioe
                .fillInStackTrace();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);

        } catch (SecurityException se) {
            String errMsg = "FileHashStore.generateTmpFile - Unable to generate tmpFile: " + se
                .fillInStackTrace();
            logFileHashStore.error(errMsg);
            throw new SecurityException(errMsg);

        }
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
     * @throws FileNotFoundException    tmnpFile cannot be found
     */
    protected Map<String, String> writeToTmpFileAndGenerateChecksums(
        File tmpFile, InputStream dataStream, String additionalAlgorithm, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, FileNotFoundException, SecurityException {
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                additionalAlgorithm, "additionalAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyString(
                checksumAlgorithm, "checksumAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(checksumAlgorithm);
        }

        FileOutputStream os = new FileOutputStream(tmpFile);
        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.getName());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.getName());
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.getName());
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.getName());
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.getName());
        MessageDigest additionalAlgo = null;
        MessageDigest checksumAlgo = null;
        if (additionalAlgorithm != null) {
            logFileHashStore.debug(
                "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding additional algorithm"
                    + " to hex digest map, algorithm: " + additionalAlgorithm
            );
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
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
                if (additionalAlgorithm != null) {
                    additionalAlgo.update(buffer, 0, bytesRead);
                }
                if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
                    checksumAlgo.update(buffer, 0, bytesRead);
                }
            }

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.writeToTmpFileAndGenerateChecksums - Unexpected Exception: " + ioe
                    .fillInStackTrace();
            logFileHashStore.error(errMsg);
            throw ioe;

        } finally {
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
        if (additionalAlgorithm != null) {
            String extraAlgoDigest = DatatypeConverter.printHexBinary(additionalAlgo.digest())
                .toLowerCase();
            hexDigests.put(additionalAlgorithm, extraAlgoDigest);
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
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
            logFileHashStore.debug(errMsg);
            throw new FileAlreadyExistsException(errMsg);
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
            logFileHashStore.debug(
                "FileHashStore.move - file moved from: " + sourceFilePath + ", to: "
                    + targetFilePath
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
     * Writes the given 'pid' into a tmp file in the cid refs file format, which consists of
     * multiple pids that references a 'cid' delimited by "\n".
     *
     * @param pid Authority-based or persistent identifier to write
     * @throws IOException Failure to write pid refs file
     */
    protected File writeCidRefsFile(String pid) throws IOException {
        File cidRefsTmpFile = generateTmpFile("tmp", REFS_TMP_FILE_DIRECTORY);
        String pidNewLine = pid + "\n";

        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(cidRefsTmpFile.toPath()), StandardCharsets.UTF_8
            )
        )) {
            writer.write(pidNewLine);
            writer.close();

            return cidRefsTmpFile;

        } catch (IOException ioe) {
            logFileHashStore.error(
                "FileHashStore.writeCidRefsFile - Unable to write cid refs file for pid: " + pid
                    + " IOException: " + ioe.getMessage()
            );
            throw ioe;
        }
    }

    /**
     * Writes the given 'cid' into a tmp file in the 'pid' refs file format. A pid refs file
     * contains a single 'cid'. Note, a 'pid' can only ever reference one 'cid'.
     * 
     * @param cid Content identifier to write
     * @throws IOException Failure to write pid refs file
     */
    protected File writePidRefsFile(String cid) throws IOException {
        File pidRefsTmpFile = generateTmpFile("tmp", REFS_TMP_FILE_DIRECTORY);
        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(pidRefsTmpFile.toPath()), StandardCharsets.UTF_8
            )
        )) {
            writer.write(cid);
            writer.close();

            return pidRefsTmpFile;

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStore.writePidRefsFile - Unable to write pid refs file for cid: " + cid
                    + " IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Checks a given cid refs file for a pid.
     * 
     * @param pid            Authority-based or persistent identifier to search
     * @param absCidRefsPath Path to the cid refs file to check
     * @return True if cid is found, false otherwise
     * @throws IOException If unable to read the cid refs file.
     */
    protected boolean isPidInCidRefsFile(String pid, Path absCidRefsPath) throws IOException {
        List<String> lines = Files.readAllLines(absCidRefsPath);
        boolean pidFoundInCidRefFiles = false;
        for (String line : lines) {
            if (line.equals(pid)) {
                pidFoundInCidRefFiles = true;
                break;
            }
        }
        return pidFoundInCidRefFiles;
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
        // First check that the files exist
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
            boolean pidFoundInCidRefFiles = isPidInCidRefsFile(pid, absCidRefsPath);
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
     * Updates a cid refs file with a pid that references the cid
     * 
     * @param pid            Authority-based or persistent identifier
     * @param absCidRefsPath Path to the cid refs file to update
     * @throws IOException Issue with updating a cid refs file
     */
    protected void updateCidRefsFiles(String pid, Path absCidRefsPath) throws IOException {
        File absPathCidRefsFile = absCidRefsPath.toFile();
        try {
            // Obtain a lock on the file before updating it
            try (RandomAccessFile raf = new RandomAccessFile(absPathCidRefsFile, "rw");
                 FileChannel channel = raf.getChannel(); FileLock lock = channel.lock()) {

                try (BufferedWriter writer = new BufferedWriter(
                    new FileWriter(absPathCidRefsFile, true)
                )) {
                    writer.write(pid + "\n");
                    writer.close();
                }
                logFileHashStore.debug(
                    "FileHashStore.updateCidRefsFiles - Pid: " + pid
                        + " has been added to cid refs file: " + absPathCidRefsFile
                );
            }
            // The lock is automatically released when the try block exits

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.updateCidRefsFiles - " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Deletes a pid references file
     * 
     * @param pid Authority-based or persistent identifier
     * @throws NoSuchAlgorithmException Incompatible algorithm used to find pid refs file
     * @throws IOException              Unable to delete object or open pid refs file
     */
    protected void deletePidRefsFile(String pid) throws NoSuchAlgorithmException, IOException {
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deletePidRefsFile");
        FileHashStoreUtility.checkForEmptyString(pid, "pid", "deletePidRefsFile");

        Path absPidRefsPath = getRealPath(pid, "refs", "pid");

        // Check to see if pid refs file exists
        if (!Files.exists(absPidRefsPath)) {
            String errMsg =
                "FileHashStore.deletePidRefsFile - File refs file does not exist for pid: " + pid
                    + " with address" + absPidRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);

        } else {
            // Proceed to delete
            Files.delete(absPidRefsPath);
            logFileHashStore.debug(
                "FileHashStore.deletePidRefsFile - Pid refs file deleted for: " + pid
                    + " with address: " + absPidRefsPath
            );
        }
    }


    /**
     * Removes a pid from a cid refs file.
     * 
     * @param pid Authority-based or persistent identifier.
     * @param cid Content identifier
     * @throws IOException Unable to access cid refs file
     */
    protected void deleteCidRefsPid(String pid, String cid) throws NoSuchAlgorithmException,
        IOException {
        FileHashStoreUtility.ensureNotNull(cid, "pid", "deleteCidRefsPid");
        FileHashStoreUtility.checkForEmptyString(cid, "pid", "deleteCidRefsPid");

        Path absCidRefsPath = getRealPath(cid, "refs", "cid");

        // Check to see if cid refs file exists
        if (!Files.exists(absCidRefsPath)) {
            String errMsg =
                "FileHashStore.deleteCidRefsPid - Cid refs file does not exist for cid: " + cid
                    + " with address" + absCidRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);

        } else {
            if (isPidInCidRefsFile(pid, absCidRefsPath)) {
                try {
                    List<String> lines = new ArrayList<>(Files.readAllLines(absCidRefsPath));
                    lines.remove(pid);
                    Files.write(
                        absCidRefsPath, lines, StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING
                    );
                    logFileHashStore.debug(
                        "FileHashStore.deleteCidRefsPid - Pid: " + pid
                            + " removed from cid refs file: " + absCidRefsPath
                    );

                } catch (IOException ioe) {
                    String errMsg = "FileHashStore.deleteCidRefsPid - Unable to remove pid: " + pid
                        + "from cid refs file: " + absCidRefsPath + ". Additional Info: " + ioe
                            .getMessage();
                    logFileHashStore.error(errMsg);
                    throw new IOException(errMsg);
                }
                // Perform clean up on cid refs file - if it is empty, delete it
                if (Files.size(absCidRefsPath) == 0) {
                    Files.delete(absCidRefsPath);
                }

            } else {
                String errMsg = "FileHashStore.deleteCidRefsPid - pid: " + pid
                    + " not found in cid refs file: " + absCidRefsPath;
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }
    }


    /**
     * Deletes a cid refs file if it is empty.
     * 
     * @param cid Content identifier
     * @throws IOException Unable to delete object cid refs file
     */
    protected void deleteCidRefsFile(String cid) throws NoSuchAlgorithmException, IOException {
        FileHashStoreUtility.ensureNotNull(cid, "pid", "deleteCidRefsFile");
        FileHashStoreUtility.checkForEmptyString(cid, "pid", "deleteCidRefsFile");

        Path absCidRefsPath = getRealPath(cid, "refs", "cid");

        // Check to see if cid refs file exists
        if (!Files.exists(absCidRefsPath)) {
            String errMsg =
                "FileHashStore.deleteCidRefsFile - Cid refs file does not exist for cid: " + cid
                    + " with address" + absCidRefsPath;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);

        } else {
            // A cid refs file is only deleted if it is empty. Client must remove pid(s) first
            if (Files.size(absCidRefsPath) == 0) {
                Files.delete(absCidRefsPath);
                logFileHashStore.debug(
                    "FileHashStore.deleteCidRefsFile - Deleted cid refs file: " + absCidRefsPath
                );

            } else {
                String errMsg =
                    "FileHashStore.deleteCidRefsFile - Unable to delete cid refs file, it is not empty: "
                        + absCidRefsPath;
                logFileHashStore.warn(errMsg);
            }
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
            "FileHashStore.putMetadata - Called to put metadata for pid:" + pid
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
        String metadataCid = getPidHexDigest(pid + checkedFormatId, OBJECT_STORE_ALGORITHM);
        Path metadataCidPath = getRealPath(pid, "metadata", checkedFormatId);

        // Store metadata to tmpMetadataFile
        File tmpMetadataFile = generateTmpFile("tmp", METADATA_TMP_FILE_DIRECTORY);
        boolean tmpMetadataWritten = writeToTmpMetadataFile(tmpMetadataFile, metadata);
        if (tmpMetadataWritten) {
            logFileHashStore.debug(
                "FileHashStore.putMetadata - tmp metadata file has been written, moving to"
                    + " permanent location: " + metadataCidPath
            );
            File permMetadataFile = metadataCidPath.toFile();
            move(tmpMetadataFile, permMetadataFile, "metadata");
        }
        logFileHashStore.debug(
            "FileHashStore.putMetadata - Move metadata success, permanent address: "
                + metadataCidPath
        );
        return metadataCid;
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
     * Get the absolute path of a HashStore object or metadata file
     *
     * @param abId     Authority-based, persistent or content identifier
     * @param entity   "object" or "metadata"
     * @param formatId Metadata namespace or reference type (pid/cid)
     * @return Actual path to object
     * @throws IllegalArgumentException If entity is not object or metadata
     * @throws NoSuchAlgorithmException If store algorithm is not supported
     * @throws IOException              If unable to retrieve cid
     */
    protected Path getRealPath(String abId, String entity, String formatId)
        throws IllegalArgumentException, NoSuchAlgorithmException, IOException {
        Path realPath;
        if (entity.equalsIgnoreCase("object")) {
            // 'abId' is expected to be a pid
            String objectCid = findObject(abId);
            String objShardString = getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
            );
            realPath = OBJECT_STORE_DIRECTORY.resolve(objShardString);

        } else if (entity.equalsIgnoreCase("metadata")) {
            String objectCid = getPidHexDigest(abId + formatId, OBJECT_STORE_ALGORITHM);
            String objShardString = getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
            );
            realPath = METADATA_STORE_DIRECTORY.resolve(objShardString);

        } else if (entity.equalsIgnoreCase("refs")) {
            if (formatId.equalsIgnoreCase("pid")) {
                String pidRefId = getPidHexDigest(abId, OBJECT_STORE_ALGORITHM);
                String pidShardString = getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, pidRefId
                );
                realPath = REFS_PID_FILE_DIRECTORY.resolve(pidShardString);
            } else if (formatId.equalsIgnoreCase("cid")) {
                String cidShardString = getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, abId
                );
                realPath = REFS_CID_FILE_DIRECTORY.resolve(cidShardString);
            } else {
                String errMsg =
                    "FileHashStore.getRealPath - formatId must be 'pid' or 'cid' when entity is 'refs'";
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }

        } else {
            throw new IllegalArgumentException(
                "FileHashStore.getRealPath - entity must be 'object' or 'metadata'"
            );
        }
        return realPath;
    }
}

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
import java.util.Collection;
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
import org.dataone.hashstore.exceptions.CidNotFoundInPidRefsFileException;
import org.dataone.hashstore.exceptions.HashStoreRefsAlreadyExistException;
import org.dataone.hashstore.exceptions.MissingHexDigestsException;
import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.OrphanPidRefsFileException;
import org.dataone.hashstore.exceptions.OrphanRefsFilesException;
import org.dataone.hashstore.exceptions.PidNotFoundInCidRefsFileException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.exceptions.PidRefsFileNotFoundException;
import org.dataone.hashstore.exceptions.UnsupportedHashAlgorithmException;

/**
 * FileHashStore is a HashStore adapter class that manages the storage of objects and metadata to a
 * given store path on disk. To instantiate FileHashStore, the calling app must provide predefined
 * properties as described by FileHashStore's single constructor.
 */
public class FileHashStore implements HashStore {
    private static final Log logFileHashStore = LogFactory.getLog(FileHashStore.class);
    private static final int TIME_OUT_MILLISEC = 1000;
    private static final Collection<String> objectLockedCids = new ArrayList<>(100);
    private static final Collection<String> objectLockedPids = new ArrayList<>(100);
    private static final Collection<String> metadataLockedDocIds = new ArrayList<>(100);
    private static final Collection<String> referenceLockedPids = new ArrayList<>(100);
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

    public enum DefaultHashAlgorithms {
        MD5("MD5"), SHA_1("SHA-1"), SHA_256("SHA-256"), SHA_384("SHA-384"), SHA_512("SHA-512");

        final String algoName;

        DefaultHashAlgorithms(String algo) {
            algoName = algo;
        }

        public String getName() {
            return algoName;
        }
    }

    public enum HashStoreProperties {
        storePath, storeDepth, storeWidth, storeAlgorithm, storeMetadataNamespace
    }

    /**
     * Constructor to initialize FileHashStore, properties are required. FileHashStore is not
     * responsible for ensuring that the given store path is accurate. Upon initialization, if
     * an existing config file (hashstore.yaml) is present, it will confirm that it is accurate
     * against the supplied properties. If not, FileHashSTore will check for 'hashstore' specific
     * directories at the supplied store path before initializing.
     *
     * @param hashstoreProperties Properties object with the following keys: storePath, storeDepth,
     *                            storeWidth, storeAlgorithm, storeMetadataNamespace
     * @throws IllegalArgumentException Constructor arguments cannot be null, empty or less than 0
     * @throws IOException              Issue with creating directories
     * @throws NoSuchAlgorithmException Unsupported store algorithm
     */
    public FileHashStore(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        logFileHashStore.info("Initializing FileHashStore");
        FileHashStoreUtility.ensureNotNull(
            hashstoreProperties, "hashstoreProperties", "FileHashStore - constructor"
        );

        // Get properties
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

        verifyHashStoreProperties(
            storePath, storeDepth, storeWidth, storeAlgorithm, storeMetadataNamespace
        );

        // HashStore configuration has been reviewed, proceed with initialization
        STORE_ROOT = storePath;
        DIRECTORY_DEPTH = storeDepth;
        DIRECTORY_WIDTH = storeWidth;
        OBJECT_STORE_ALGORITHM = storeAlgorithm;
        DEFAULT_METADATA_NAMESPACE = storeMetadataNamespace;
        OBJECT_STORE_DIRECTORY = storePath.resolve("objects");
        METADATA_STORE_DIRECTORY = storePath.resolve("metadata");
        REFS_STORE_DIRECTORY = storePath.resolve("refs");
        OBJECT_TMP_FILE_DIRECTORY = OBJECT_STORE_DIRECTORY.resolve("tmp");
        METADATA_TMP_FILE_DIRECTORY = METADATA_STORE_DIRECTORY.resolve("tmp");
        REFS_TMP_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("tmp");
        REFS_PID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("pids");
        REFS_CID_FILE_DIRECTORY = REFS_STORE_DIRECTORY.resolve("cids");

        try {
            Files.createDirectories(OBJECT_STORE_DIRECTORY);
            Files.createDirectories(METADATA_STORE_DIRECTORY);
            Files.createDirectories(REFS_STORE_DIRECTORY);
            Files.createDirectories(OBJECT_TMP_FILE_DIRECTORY);
            Files.createDirectories(METADATA_TMP_FILE_DIRECTORY);
            Files.createDirectories(REFS_TMP_FILE_DIRECTORY);
            Files.createDirectories(REFS_PID_FILE_DIRECTORY);
            Files.createDirectories(REFS_CID_FILE_DIRECTORY);
            logFileHashStore.debug("Created store and store tmp directories.");

        } catch (IOException ioe) {
            logFileHashStore.fatal("Failed to initialize FileHashStore - unable to create"
                                       + " directories. Exception: " + ioe.getMessage());
            throw ioe;
        }
        logFileHashStore.debug(
            "HashStore initialized. Store Depth: " + DIRECTORY_DEPTH + ". Store Width: "
                + DIRECTORY_WIDTH + ". Store Algorithm: " + OBJECT_STORE_ALGORITHM
                + ". Store Metadata Namespace: " + DEFAULT_METADATA_NAMESPACE);

        // Write configuration file 'hashstore.yaml' to store HashStore properties
        Path hashstoreYaml = STORE_ROOT.resolve(HASHSTORE_YAML);
        if (!Files.exists(hashstoreYaml)) {
            String hashstoreYamlContent = buildHashStoreYamlString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, OBJECT_STORE_ALGORITHM, DEFAULT_METADATA_NAMESPACE
            );
            writeHashStoreYaml(hashstoreYamlContent);
            logFileHashStore.info(
                "hashstore.yaml written to storePath: " + hashstoreYaml);
        } else {
            logFileHashStore.info("hashstore.yaml exists and has been verified."
                                      + " Initializing FileHashStore.");
        }
    }

    // Configuration and Initialization Related Methods

    /**
     * Determines whether FileHashStore can instantiate by validating a set of arguments and
     * throwing exceptions. If HashStore configuration file (`hashstore.yaml`) exists, it will
     * retrieve its properties and compare them with the given values; and if there is a
     * mismatch, an exception will be thrown. If not, it will look to see if any relevant
     * HashStore directories exist (i.e. '/objects', '/metadata', '/refs') in the given store
     * path and throw an exception if any of those directories exist.
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
            String errMsg =
                "Depth and width must be > than 0. Depth: " + storeDepth + ". Width: " + storeWidth;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        validateAlgorithm(storeAlgorithm);
        FileHashStoreUtility.ensureNotNull(
            storeMetadataNamespace, "storeMetadataNamespace", "FileHashStore - constructor"
        );
        FileHashStoreUtility.checkForEmptyAndValidString(
            storeMetadataNamespace, "storeMetadataNamespace", "FileHashStore - constructor"
        );

        // Check to see if configuration exists before initializing
        Path hashstoreYamlPredictedPath = Paths.get(storePath + "/hashstore.yaml");
        if (Files.exists(hashstoreYamlPredictedPath)) {
            logFileHashStore.debug("hashstore.yaml found, checking properties.");

            HashMap<String, Object> hsProperties = loadHashStoreYaml(storePath);
            int existingStoreDepth = (int) hsProperties.get(HashStoreProperties.storeDepth.name());
            int existingStoreWidth = (int) hsProperties.get(HashStoreProperties.storeWidth.name());
            String existingStoreAlgorithm = (String) hsProperties.get(
                HashStoreProperties.storeAlgorithm.name()
            );
            String existingStoreMetadataNs = (String) hsProperties.get(
                HashStoreProperties.storeMetadataNamespace.name()
            );

            FileHashStoreUtility.checkObjectEquality("store depth", storeDepth, existingStoreDepth);
            FileHashStoreUtility.checkObjectEquality("store width", storeWidth, existingStoreWidth);
            FileHashStoreUtility.checkObjectEquality("store algorithm", storeAlgorithm,
                                                     existingStoreAlgorithm);
            FileHashStoreUtility.checkObjectEquality(
                "store metadata namespace", storeMetadataNamespace, existingStoreMetadataNs);
            logFileHashStore.info("hashstore.yaml found and HashStore verified");

        } else {
            // Check if HashStore related folders exist at the given store path
            logFileHashStore.debug("hashstore.yaml not found, checking store path for"
                                       + " `/objects`, `/metadata` and `/refs` directories.");
            if (Files.isDirectory(storePath)) {
                Path[] conflictingDirectories = {
                    storePath.resolve("objects"),
                    storePath.resolve("metadata"),
                    storePath.resolve("refs")
                };
                for (Path dir : conflictingDirectories) {
                    if (Files.exists(dir) && Files.isDirectory(dir)) {
                        String errMsg = "FileHashStore - Unable to initialize HashStore."
                            + "`hashstore.yaml` is not found but potential conflicting"
                            + " directory exists: " + dir + ". Please choose a new folder or"
                            + " delete the conflicting directory and try again.";
                        logFileHashStore.fatal(errMsg);
                        throw new IllegalStateException(errMsg);
                    }
                }
            }
            logFileHashStore.debug("hashstore.yaml not found. Supplied properties accepted.");
        }
    }

    /**
     * Get the properties of HashStore from an existing 'hashstore.yaml'
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
                " Unable to retrieve 'hashstore.yaml'. IOException: " + ioe.getMessage());
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
                "Unable to write 'hashstore.yaml'. IOException: " + ioe.getMessage());
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
                + "store_metadata_namespace: \"%s\"\n"
                + "############### Hash Algorithms ###############\n"
                + "# Hash algorithm to use when calculating object's hex digest "
                + "for the permanent address\n" + "store_algorithm: \"%s\"\n"
                + "############### Hash Algorithms ###############\n"
                + "# Hash algorithm to use when calculating object's hex digest "
                + "for the permanent address\n"
                + "# The default algorithm list includes the hash algorithms "
                + "calculated when storing an\n"
                + "# object to disk and returned to the caller after successful " + "storage.\n"
                + "store_default_algo_list:\n" + "- \"MD5\"\n" + "- \"SHA-1\"\n" + "- \"SHA-256\"\n"
                + "- \"SHA-384\"\n" + "- \"SHA-512\"\n", storeDepth, storeWidth, storeMetadataNamespace, storeAlgorithm
        );
    }

    // HashStore Public API Methods

    @Override
    public ObjectMetadata storeObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws NoSuchAlgorithmException, IOException, RuntimeException, InterruptedException,
        PidRefsFileExistsException {
        logFileHashStore.debug("Storing data object for pid: " + pid);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(object, "object", "storeObject");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "storeObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "storeObject");
        // Validate algorithms if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                additionalAlgorithm, "additionalAlgorithm", "storeObject");
            validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksumAlgorithm, "checksumAlgorithm", "storeObject");
            validateAlgorithm(checksumAlgorithm);
        }
        if (objSize != -1) {
            FileHashStoreUtility.checkNotNegativeOrZero(objSize, "storeObject");
        }

        try {
            return syncPutObject(
                object, pid, additionalAlgorithm, checksum, checksumAlgorithm, objSize
            );
        } finally {
            // Close stream
            object.close();
        }
    }

    /**
     * Method to synchronize storing objects with FileHashStore
     */
    private ObjectMetadata syncPutObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws NoSuchAlgorithmException, PidRefsFileExistsException, IOException, RuntimeException,
        InterruptedException {
        try {
            // Lock pid for thread safety, transaction control and atomic writing
            // An object is stored once and only once
            synchronized (objectLockedPids) {
                if (objectLockedPids.contains(pid)) {
                    String errMsg = "Duplicate object request encountered for pid: " + pid
                        + ". Already in progress.";
                    logFileHashStore.warn(errMsg);
                    throw new RuntimeException(errMsg);
                }
                logFileHashStore.debug("Synchronizing objectLockedPids for pid: " + pid);
                objectLockedPids.add(pid);
            }

            logFileHashStore.debug(
                "putObject() called to store pid: " + pid + ". additionalAlgorithm: "
                    + additionalAlgorithm + ". checksum: " + checksum + ". checksumAlgorithm: "
                    + checksumAlgorithm);
            // Store object
            ObjectMetadata objInfo = putObject(
                object, pid, additionalAlgorithm, checksum, checksumAlgorithm, objSize
            );
            // Tag object
            String cid = objInfo.getCid();
            tagObject(pid, cid);
            objInfo.setPid(pid);
            logFileHashStore.info(
                "Object stored for pid: " + pid + " at " + getHashStoreDataObjectPath(pid));
            return objInfo;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg =
                "Unable to store object for pid: " + pid + ". NoSuchAlgorithmException: "
                    + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } catch (PidRefsFileExistsException prfee) {
            String errMsg =
                "Unable to store object for pid: " + pid + ". PidRefsFileExistsException: "
                    + prfee.getMessage();
            logFileHashStore.error(errMsg);
            throw prfee;

        } catch (IOException ioe) {
            // Covers AtomicMoveNotSupportedException, FileNotFoundException
            String errMsg =
                "Unable to store object for pid: " + pid + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (RuntimeException re) {
            // Covers SecurityException, IllegalArgumentException, NullPointerException
            String errMsg = "Unable to store object for pid: " + pid + ". Runtime Exception: "
                + re.getMessage();
            logFileHashStore.error(errMsg);
            throw re;

        } finally {
            // Release lock
            releaseObjectLockedPids(pid);
        }
    }

    /**
     * Overload method for storeObject with just an InputStream
     */
    @Override
    public ObjectMetadata storeObject(InputStream object) throws NoSuchAlgorithmException,
        IOException, PidRefsFileExistsException, RuntimeException, InterruptedException {
        // 'putObject' is called directly to bypass the pid synchronization implemented to
        // efficiently handle object store requests without a pid. This scenario occurs when
        // metadata about the object (ex. form data including the pid, checksum, checksum
        // algorithm, etc.) is unavailable.
        //
        // Note: This method does not tag the object to make it discoverable, so the client can
        // call 'deleteInvalidObject' (optional) to check that the object is valid, and then
        // 'tagObject' (required) to create the reference files needed to associate the
        // respective pids/cids.
        try {
            return putObject(object, "HashStoreNoPid", null, null, null, -1);
        } finally {
            // Close stream
            object.close();
        }
    }


    @Override
    public void tagObject(String pid, String cid) throws IOException, PidRefsFileExistsException,
        NoSuchAlgorithmException, FileNotFoundException, InterruptedException {
        logFileHashStore.debug("Tagging cid (" + cid + ") with pid: " + pid);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "tagObject");
        FileHashStoreUtility.ensureNotNull(cid, "cid", "tagObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "tagObject");
        FileHashStoreUtility.checkForEmptyAndValidString(cid, "cid", "tagObject");

        try {
            synchronizeObjectLockedCids(cid);
            synchronizeReferenceLockedPids(pid);
            storeHashStoreRefsFiles(pid, cid);

        } catch (HashStoreRefsAlreadyExistException hsrfae) {
            // This exception is thrown when the pid and cid are already tagged appropriately
            String errMsg =
                "HashStore refs files already exist for pid " + pid + " and cid: " + cid;
            throw new HashStoreRefsAlreadyExistException(errMsg);

        } catch (PidRefsFileExistsException prfe) {
            String errMsg = "pid: " + pid + " already references another cid."
                + " A pid can only reference one cid.";
            throw new PidRefsFileExistsException(errMsg);

        } catch (Exception e) {
            // Revert the process for all other exceptions
            // We must first release the cid and pid since 'unTagObject' is synchronized
            // If not, we will run into a deadlock.
            releaseObjectLockedCids(cid);
            releaseReferenceLockedPids(pid);
            unTagObject(pid, cid);
            throw e;

        } finally {
            // Release locks
            releaseObjectLockedCids(cid);
            releaseReferenceLockedPids(pid);
        }
    }

    @Override
    public String storeMetadata(InputStream metadata, String pid, String formatId)
        throws IOException, IllegalArgumentException, FileNotFoundException, InterruptedException,
        NoSuchAlgorithmException {
        logFileHashStore.debug("Storing metadata for pid: " + pid + ", with formatId: " + formatId);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(metadata, "metadata", "storeMetadata");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "storeMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "storeMetadata");

        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = DEFAULT_METADATA_NAMESPACE;
        } else {
            FileHashStoreUtility.checkForEmptyAndValidString(formatId, "formatId", "storeMetadata");
            checkedFormatId = formatId;
        }

        try {
            return syncPutMetadata(metadata, pid, checkedFormatId);
        } finally {
            // Close stream
            metadata.close();
        }
    }

    /**
     * Method to synchronize storing metadata with FileHashStore
     */
    private String syncPutMetadata(InputStream metadata, String pid, String checkedFormatId)
        throws InterruptedException, IOException, NoSuchAlgorithmException {
        // Get the metadata document id, which is the synchronization value
        String pidFormatId = pid + checkedFormatId;
        String metadataDocId = FileHashStoreUtility.getPidHexDigest(pidFormatId,
                                                                OBJECT_STORE_ALGORITHM);
        logFileHashStore.debug(
            "putMetadata() called to store metadata for pid: " + pid + ", with formatId: "
                + checkedFormatId + " for metadata document: " + metadataDocId);
        try {
            synchronizeMetadataLockedDocIds(metadataDocId);
            // Store metadata
            String pathToStoredMetadata = putMetadata(metadata, pid, checkedFormatId);
            logFileHashStore.info(
                "Metadata stored for pid: " + pid + " at: " + pathToStoredMetadata);
            return pathToStoredMetadata;

        } catch (IOException ioe) {
            String errMsg =
                "Unable to store metadata, IOException encountered: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "Unable to store metadata, algorithm to calculate"
                + " permanent address is not supported: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } finally {
            releaseMetadataLockedDocIds(metadataDocId);
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
        logFileHashStore.debug("Retrieving InputStream to data object for pid: " + pid);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "retrieveObject");

        // Check to see if object exists
        Path objRealPath = getHashStoreDataObjectPath(pid);
        if (!Files.exists(objRealPath)) {
            String errMsg =
                "File does not exist for pid: " + pid + " with object address: " + objRealPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // Return an InputStream to read from the data object
        try {
            InputStream objectCidInputStream = Files.newInputStream(objRealPath);
            logFileHashStore.info("Retrieved object for pid: " + pid);
            return objectCidInputStream;

        } catch (IOException ioe) {
            String errMsg =
                "Unexpected error when creating InputStream for pid: " + pid + ", IOException: "
                    + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }

    }

    @Override
    public InputStream retrieveMetadata(String pid, String formatId)
        throws IllegalArgumentException, FileNotFoundException, IOException,
        NoSuchAlgorithmException {
        logFileHashStore.debug(
            "Retrieving metadata document for pid: " + pid + " with formatId: " + formatId);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.ensureNotNull(formatId, "formatId", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(formatId, "formatId", "retrieveMetadata");

        return getHashStoreMetadataInputStream(pid, formatId);
    }

    /**
     * Overload method for retrieveMetadata with default metadata namespace
     */
    @Override
    public InputStream retrieveMetadata(String pid) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug(
            "Retrieving metadata for pid: " + pid + " with default metadata namespace: ");
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "retrieveMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "retrieveMetadata");

        return getHashStoreMetadataInputStream(pid, DEFAULT_METADATA_NAMESPACE);
    }

    @Override
    public void deleteObject(String pid)
        throws IllegalArgumentException, IOException, NoSuchAlgorithmException,
        InterruptedException {
        logFileHashStore.debug("Deleting object for pid: " + pid);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "id", "deleteObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "id", "deleteObject");
        Collection<Path> deleteList = new ArrayList<>();

        try {
            // Storing, deleting and untagging objects are synchronized together
            // Duplicate store object requests for a pid are rejected, but deleting an object
            // will wait for a pid to be released if it's found to be in use before proceeding.
            synchronizeObjectLockedPids(pid);

            // Before we begin deletion process, we look for the `cid` by calling
            // `findObject` which will throw custom exceptions if there is an issue with
            // the reference files, which help us determine the path to proceed with.
            try {
                Map<String, String> objInfoMap = findObject(pid);
                String cid = objInfoMap.get("cid");

                // If no exceptions are thrown, we proceed to synchronization based on the `cid`
                synchronizeObjectLockedCids(cid);

                try {
                    // Proceed with comprehensive deletion - cid exists, nothing out of place
                    Path absCidRefsPath = getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
                    Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());

                    // Begin deletion process
                    updateRefsFile(pid, absCidRefsPath, "remove");
                    if (Files.size(absCidRefsPath) == 0) {
                        Path objRealPath = getHashStoreDataObjectPath(pid);
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(objRealPath));
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                    } else {
                        String warnMsg = "cid referenced by pid: " + pid
                            + " is not empty (refs exist for cid). Skipping object deletion.";
                        logFileHashStore.warn(warnMsg);
                    }
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete all related/relevant items with the least amount of delay
                    FileHashStoreUtility.deleteListItems(deleteList);
                    deleteMetadata(pid);
                    logFileHashStore.info("Data file and references deleted for: " + pid);

                } finally {
                    // Release lock
                    releaseObjectLockedCids(cid);
                }

            } catch (OrphanPidRefsFileException oprfe) {
                // `findObject` throws this exception when the cid refs file doesn't exist,
                // so we only need to delete the pid refs file and related metadata documents
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                // Delete items
                FileHashStoreUtility.deleteListItems(deleteList);
                deleteMetadata(pid);
                String warnMsg = "Cid refs file does not exist for pid: " + pid
                    + ". Deleted orphan pid refs file and metadata.";
                logFileHashStore.warn(warnMsg);

            } catch (OrphanRefsFilesException orfe) {
                // `findObject` throws this exception when the pid and cid refs file exists,
                // but the actual object being referenced by the pid does not exist
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                String cidRead = new String(Files.readAllBytes(absPidRefsPath));

                try {
                    // Since we must access the cid reference file, the `cid` must be synchronized
                    synchronizeObjectLockedCids(cidRead);

                    Path absCidRefsPath =
                        getHashStoreRefsPath(cidRead, HashStoreIdTypes.cid.getName());
                    updateRefsFile(pid, absCidRefsPath, "remove");
                    if (Files.size(absCidRefsPath) == 0) {
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                    }
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete items
                    FileHashStoreUtility.deleteListItems(deleteList);
                    deleteMetadata(pid);
                    String warnMsg = "Object with cid: " + cidRead
                        + " does not exist, but pid and cid reference file found for pid: " + pid
                        + ". Deleted pid and cid ref files and metadata.";
                    logFileHashStore.warn(warnMsg);

                } finally {
                    // Release lock
                    releaseObjectLockedCids(cidRead);
                }
            } catch (PidNotFoundInCidRefsFileException pnficrfe) {
                // `findObject` throws this exception when both the pid and cid refs file exists
                // but the pid is not found in the cid refs file.
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                FileHashStoreUtility.deleteListItems(deleteList);
                deleteMetadata(pid);
                String warnMsg = "Pid not found in expected cid refs file for pid: " + pid
                    + ". Deleted orphan pid refs file and metadata.";
                logFileHashStore.warn(warnMsg);
            }
        } finally {
            // Release lock
            releaseObjectLockedPids(pid);
        }
    }


    @Override
    public void deleteInvalidObject(
        ObjectMetadata objectInfo, String checksum, String checksumAlgorithm, long objSize)
        throws NonMatchingObjSizeException, NonMatchingChecksumException,
        UnsupportedHashAlgorithmException, InterruptedException, NoSuchAlgorithmException,
        IOException {
        logFileHashStore.debug("Verifying data object for cid: " + objectInfo.getCid());
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(objectInfo, "objectInfo", "deleteInvalidObject");
        FileHashStoreUtility.ensureNotNull(
            objectInfo.getHexDigests(), "objectInfo.getHexDigests()", "deleteInvalidObject");
        if (objectInfo.getHexDigests().isEmpty()) {
            throw new MissingHexDigestsException("Missing hexDigests in supplied ObjectMetadata");
        }
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "deleteInvalidObject");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "deleteInvalidObject");
        FileHashStoreUtility.checkNotNegativeOrZero(objSize, "deleteInvalidObject");

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
                        "Unexpected error when calculating a checksum for cid: " + objCid
                            + " with algorithm (" + checksumAlgorithm
                            + ") that is not part of the default list. " + ioe.getMessage();
                    throw new IOException(errMsg);
                }
            } catch (NoSuchAlgorithmException nsae) {
                String errMsg = "checksumAlgorithm given: " + checksumAlgorithm
                    + " is not supported. Supported algorithms: " + Arrays.toString(
                    SUPPORTED_HASH_ALGORITHMS);
                logFileHashStore.error(errMsg);
                throw new UnsupportedHashAlgorithmException(errMsg);
            }
        }
        // Validate checksum
        if (!digestFromHexDigests.equals(checksum)) {
            deleteObjectByCid(objCid);
            String errMsg =
                "Object content invalid for cid: " + objCid + ". Expected checksum: " + checksum
                    + ". Actual checksum calculated: " + digestFromHexDigests + " (algorithm: "
                    + checksumAlgorithm + ")";
            logFileHashStore.error(errMsg);
            throw new NonMatchingChecksumException(errMsg);
        }
        // Validate size
        if (objInfoRetrievedSize != objSize) {
            deleteObjectByCid(objCid);
            String errMsg = "Object size invalid for cid: " + objCid + ". Expected size: " + objSize
                + ". Actual size: " + objInfoRetrievedSize;
            logFileHashStore.error(errMsg);
            throw new NonMatchingObjSizeException(errMsg);
        }

        String infoMsg =
            "Object has been validated for cid: " + objCid + ". Expected checksum: " + checksum
                + ". Actual checksum calculated: " + digestFromHexDigests + " (algorithm: "
                + checksumAlgorithm + ")";
        logFileHashStore.info(infoMsg);
    }

    @Override
    public void deleteMetadata(String pid, String formatId) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug(
            "Deleting metadata document for pid: " + pid + " with formatId: " + formatId);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.ensureNotNull(formatId, "formatId", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(formatId, "formatId", "deleteMetadata");

        // Get the path to the metadata document and add it to a list
        Path metadataDocPath = getHashStoreMetadataPath(pid, formatId);
        Collection<Path> metadataDocPaths = new ArrayList<>();
        metadataDocPaths.add(metadataDocPath);

        if (!metadataDocPaths.isEmpty()) {
            Collection<Path> deleteList = syncRenameMetadataDocForDeletion(metadataDocPaths);
            // Delete all items in the list
            FileHashStoreUtility.deleteListItems(deleteList);
        }
        logFileHashStore.info(
            "Metadata document deleted for: " + pid + " with metadata address: " + metadataDocPath);
    }

    /**
     * Overload method for deleteMetadata with default metadata namespace
     */
    @Override
    public void deleteMetadata(String pid) throws IllegalArgumentException, IOException,
        NoSuchAlgorithmException, InterruptedException {
        logFileHashStore.debug("Deleting all metadata documents for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(pid, "pid", "deleteMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "deleteMetadata");

        // Get the path to the pid metadata document directory
        String pidHexDigest = FileHashStoreUtility.getPidHexDigest(pid, OBJECT_STORE_ALGORITHM);
        String pidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, pidHexDigest
        );
        Path expectedPidMetadataDirectory = METADATA_STORE_DIRECTORY.resolve(pidRelativePath);
        // Add all metadata docs found in the metadata doc directory to a list to iterate over
        List<Path> metadataDocPaths =
            FileHashStoreUtility.getFilesFromDir(expectedPidMetadataDirectory);

        if (!metadataDocPaths.isEmpty()) {
            Collection<Path> deleteList = syncRenameMetadataDocForDeletion(metadataDocPaths);
            // Delete all items in the list
            FileHashStoreUtility.deleteListItems(deleteList);
        }
        logFileHashStore.info("All metadata documents deleted for: " + pid);
    }

    /**
     * Synchronize renaming metadata documents for deletion
     *
     * @param metadataDocPaths List of metadata document paths
     * @throws IOException          If there is an issue renaming paths
     * @throws InterruptedException If there is an issue with synchronization metadata calls
     */
    protected Collection<Path> syncRenameMetadataDocForDeletion(
        Collection<Path> metadataDocPaths) throws IOException, InterruptedException {
        FileHashStoreUtility.ensureNotNull(
            metadataDocPaths, "metadataDocPaths", "syncRenameMetadataDocForDeletion");
        if (metadataDocPaths.isEmpty()) {
            String errMsg = "metadataDocPaths supplied cannot be empty.";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        // Rename paths and add to a List
        Collection<Path> metadataDocsToDelete = new ArrayList<>();
        try {
            for (Path metadataDocToDelete : metadataDocPaths) {
                String metadataDocId = metadataDocToDelete.getFileName().toString();
                try {
                    synchronizeMetadataLockedDocIds(metadataDocId);
                    if (Files.exists(metadataDocToDelete)) {
                        metadataDocsToDelete.add(
                            FileHashStoreUtility.renamePathForDeletion(metadataDocToDelete));
                    }
                } finally {
                    releaseMetadataLockedDocIds(metadataDocId);
                }
            }
        } catch (Exception ge) {
            // If there is any exception, attempt to revert the process and throw an exception
            if (!metadataDocsToDelete.isEmpty()) {
                for (Path metadataDocToPlaceBack : metadataDocsToDelete) {
                    Path fileNameWithDeleted = metadataDocToPlaceBack.getFileName();
                    String metadataDocId = fileNameWithDeleted.toString().replace("_delete", "");
                    try {
                        synchronizeMetadataLockedDocIds(metadataDocId);
                        if (Files.exists(metadataDocToPlaceBack)) {
                            FileHashStoreUtility.renamePathForRestoration(metadataDocToPlaceBack);
                        }
                    } finally {
                        releaseMetadataLockedDocIds(metadataDocId);
                    }
                }
            }
            String errMsg = "An unexpected exception has occurred when deleting metadata "
                + "documents. Attempts to restore all affected metadata documents have "
                + "been made. Additional details: " + ge.getMessage();
            logFileHashStore.error(errMsg);
            throw ge;
        }

        return metadataDocsToDelete;
    }

    @Override
    public String getHexDigest(String pid, String algorithm) throws IllegalArgumentException,
        FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug("Calculating hex digest for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(pid, "pid", "getHexDigest");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "getHexDigest");
        validateAlgorithm(algorithm);

        // Find the content identifier
        if (algorithm.equals(OBJECT_STORE_ALGORITHM)) {
            Map<String, String> objInfoMap = findObject(pid);
            return objInfoMap.get("cid");

        } else {
            // Get permanent address of the pid object
            Path objRealPath = getHashStoreDataObjectPath(pid);
            if (!Files.exists(objRealPath)) {
                String errMsg =
                    "File does not exist for pid: " + pid + " with object address: " + objRealPath;
                logFileHashStore.warn(errMsg);
                throw new FileNotFoundException(errMsg);
            }

            InputStream dataStream = Files.newInputStream(objRealPath);
            String mdObjectHexDigest = FileHashStoreUtility.calculateHexDigest(
                dataStream, algorithm
            );
            logFileHashStore.info(
                "Hex digest calculated for pid: " + pid + ", with hex digest value: "
                    + mdObjectHexDigest
            );
            return mdObjectHexDigest;
        }
    }

    // FileHashStore Core & Supporting Methods

    /**
     * Checks whether an object referenced by a pid exists and returns a map containing the
     * absolute path to the object, pid refs file, cid refs file and sysmeta document.
     *
     * @param pid Authority-based identifier
     * @return Map containing the following keys: cid, cid_object_path, cid_refs_path,
     * pid_refs_path, sysmeta_path
     * @throws NoSuchAlgorithmException          When algorithm used to calculate pid refs
     *                                           file's absolute address is not valid
     * @throws IOException                       Unable to read from a pid refs file or pid refs
     *                                           file does not exist
     * @throws OrphanRefsFilesException          pid and cid refs file found, but object does
     *                                           not exist
     * @throws OrphanPidRefsFileException        When pid refs file exists and the cid found
     *                                           inside does not exist.
     * @throws PidNotFoundInCidRefsFileException When pid and cid ref files exists but the
     *                                           expected pid is not found in the cid refs file.
     */
    protected Map<String, String> findObject(String pid) throws NoSuchAlgorithmException,
        IOException,
        OrphanPidRefsFileException, PidNotFoundInCidRefsFileException, OrphanRefsFilesException {
        logFileHashStore.debug("Finding object for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(pid, "pid", "findObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "findObject");

        // Get path of the pid references file
        Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());

        if (Files.exists(absPidRefsPath)) {
            String cid = new String(Files.readAllBytes(absPidRefsPath));
            Path absCidRefsPath = getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());

            // Throw exception if the cid refs file doesn't exist
            if (!Files.exists(absCidRefsPath)) {
                String errMsg = "Cid refs file does not exist for cid: " + cid + " with address: "
                    + absCidRefsPath + ", but pid refs file exists.";
                logFileHashStore.error(errMsg);
                throw new OrphanPidRefsFileException(errMsg);
            }
            // If the pid is found in the expected cid refs file, and the object exists, return it
            if (isStringInRefsFile(pid, absCidRefsPath)) {
                logFileHashStore.info("cid (" + cid + ") found for pid: " + pid);

                String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                    DIRECTORY_DEPTH, DIRECTORY_WIDTH, cid
                );
                Path realPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
                if (Files.exists(realPath)) {
                    Map<String, String> objInfoMap = new HashMap<>();
                    objInfoMap.put("cid", cid);
                    objInfoMap.put("cid_object_path", realPath.toString());
                    objInfoMap.put("cid_refs_path", absCidRefsPath.toString());
                    objInfoMap.put("pid_refs_path", absPidRefsPath.toString());
                    // If the default system metadata exists, include it
                    Path metadataPidExpectedPath =
                        getHashStoreMetadataPath(pid, DEFAULT_METADATA_NAMESPACE);
                    if (Files.exists(metadataPidExpectedPath)) {
                        objInfoMap.put("sysmeta_path", metadataPidExpectedPath.toString());
                    } else {
                        objInfoMap.put("sysmeta_path", "Does not exist");
                    }
                    return objInfoMap;

                } else {
                    String errMsg = "Object with cid: " + cid
                        + " does not exist, but pid and cid reference file found for pid: " + pid;
                    logFileHashStore.error(errMsg);
                    throw new OrphanRefsFilesException(errMsg);
                }

            } else {
                String errMsg = "Pid refs file exists, but pid (" + pid
                    + ") not found in cid refs file for cid: " + cid + " with address: "
                    + absCidRefsPath;
                logFileHashStore.error(errMsg);
                throw new PidNotFoundInCidRefsFileException(errMsg);
            }

        } else {
            String errMsg =
                "Unable to find cid for pid: " + pid + ". Pid refs file does not exist at: "
                    + absPidRefsPath;
            logFileHashStore.error(errMsg);
            throw new PidRefsFileNotFoundException(errMsg);
        }
    }

    /**
     * Takes a given InputStream and writes it to its permanent address on disk based on the SHA-256
     * hex digest value of an authority based identifier, usually provided as a persistent
     * identifier (pid). If an additional algorithm is provided and supported, its respective hex
     * digest value will be included in hexDigests map. If a checksum and checksumAlgorithm is
     * provided, FileHashStore will validate the given checksum against the hex digest produced
     * of the supplied checksumAlgorithm.
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
     * @throws InterruptedException            An issue synchronizing the cid when moving object
     */
    protected ObjectMetadata putObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize
    ) throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
        PidRefsFileExistsException, IllegalArgumentException, NullPointerException,
        AtomicMoveNotSupportedException, InterruptedException {
        logFileHashStore.debug("Begin writing data object for pid: " + pid);
        // If validation is desired, checksumAlgorithm and checksum must both be present
        boolean compareChecksum = verifyChecksumParameters(checksum, checksumAlgorithm);
        // Validate additional algorithm if not null or empty, throws exception if not supported
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                additionalAlgorithm, "additionalAlgorithm", "putObject"
            );
            validateAlgorithm(additionalAlgorithm);
        }
        if (objSize != -1) {
            FileHashStoreUtility.checkNotNegativeOrZero(objSize, "putObject");
        }

        // Generate tmp file and write to it
        File tmpFile = FileHashStoreUtility.generateTmpFile("tmp", OBJECT_TMP_FILE_DIRECTORY);
        Map<String, String> hexDigests;
        try {
            hexDigests = writeToTmpFileAndGenerateChecksums(
                tmpFile, object, additionalAlgorithm, checksumAlgorithm
            );
        } catch (Exception ge) {
            // If the process to write to the tmpFile is interrupted for any reason,
            // we will delete the tmpFile.
            Files.delete(tmpFile.toPath());
            String errMsg =
                "Unexpected Exception while storing object for pid: " + pid + ". " + ge.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }

        // Validate object if checksum and checksum algorithm is passed
        validateTmpObject(
            compareChecksum, checksum, checksumAlgorithm, tmpFile, hexDigests, objSize);

        // Gather the elements to form the permanent address
        String objectCid = hexDigests.get(OBJECT_STORE_ALGORITHM);
        String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
        );
        Path objRealPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);

        try {
            synchronizeObjectLockedCids(objectCid);
            // Confirm that the object does not yet exist, delete tmpFile if so
            if (!Files.exists(objRealPath)) {
                logFileHashStore.info("Storing tmpFile: " + tmpFile);
                // Move object
                File permFile = objRealPath.toFile();
                move(tmpFile, permFile, "object");
                logFileHashStore.debug("Successfully moved data object: " + objRealPath);
            } else {
                Files.delete(tmpFile.toPath());
                String errMsg =
                    "File already exists for pid: " + pid + ". Object address: " + objRealPath
                        + ". Deleting temporary file: " + tmpFile;
                logFileHashStore.warn(errMsg);
            }
        } catch (Exception e) {
            String errMsg =
                "Unexpected exception when moving object with cid: " + objectCid + " for pid:"
                    + pid + ". Additional Details: " + e.getMessage();
            logFileHashStore.error(errMsg);
            throw e;
        } finally {
            releaseObjectLockedCids(objectCid);
        }

        return new ObjectMetadata(pid, objectCid, Files.size(objRealPath), hexDigests);
    }

    /**
     * If compareChecksum is true, determines the integrity of an object with a given checksum &
     * algorithm against a list of hex digests. If there is a mismatch, the tmpFile will be deleted
     * and exceptions will be thrown.
     *
     * @param compareChecksum Decide whether to proceed with comparing checksums
     * @param checksum          Expected checksum value of object
     * @param checksumAlgorithm Hash algorithm of checksum value
     * @param tmpFile           Path to the file that is being evaluated
     * @param hexDigests        Map of the hex digests to parse data from
     * @param expectedSize      Expected size of object
     * @throws NoSuchAlgorithmException If algorithm requested to validate against is absent
     */
    protected void validateTmpObject(
        boolean compareChecksum, String checksum, String checksumAlgorithm, File tmpFile,
        Map<String, String> hexDigests, long expectedSize
    ) throws NoSuchAlgorithmException, NonMatchingChecksumException, NonMatchingObjSizeException,
        IOException {
        if (expectedSize > 0) {
            long storedObjFileSize = Files.size(Paths.get(tmpFile.toString()));
            if (expectedSize != storedObjFileSize) {
                // Delete tmp File
                try {
                    Files.delete(tmpFile.toPath());

                } catch (Exception ge) {
                    String errMsg =
                        "objSize given is not equal to the stored object size. ObjSize: " + expectedSize
                            + ". storedObjFileSize: " + storedObjFileSize
                            + ". Failed to delete tmpFile: " + tmpFile + ". " + ge.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new NonMatchingObjSizeException(errMsg);
                }

                String errMsg =
                    "objSize given is not equal to the stored object size. ObjSize: " + expectedSize
                        + ". storedObjFileSize: " + storedObjFileSize + ". Deleting tmpFile: "
                        + tmpFile;
                logFileHashStore.error(errMsg);
                throw new NonMatchingObjSizeException(errMsg);
            }
        }

        if (compareChecksum) {
            logFileHashStore.info("Validating object, checksum arguments supplied and valid.");
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (digestFromHexDigests == null) {
                String baseErrMsg = "Object cannot be validated. Algorithm not found in given "
                    + "hexDigests map. Algorithm requested: " + checksumAlgorithm;
                try {
                    Files.delete(tmpFile.toPath());
                } catch (Exception ge) {
                    String errMsg = baseErrMsg + ". Failed to delete tmpFile: " + tmpFile + ". "
                        + ge.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new NonMatchingChecksumException(errMsg);
                }
                String errMsg = baseErrMsg + ". tmpFile has been deleted: " + tmpFile;
                logFileHashStore.error(errMsg);
                throw new NoSuchAlgorithmException(errMsg);
            }

            if (!checksum.equalsIgnoreCase(digestFromHexDigests)) {
                String baseErrMsg = "Checksum given is not equal to the calculated hex digest: "
                    + digestFromHexDigests + ". Checksum" + " provided: " + checksum;
                try {
                    Files.delete(tmpFile.toPath());
                } catch (Exception ge) {
                    String errMsg = baseErrMsg + ". Failed to delete tmpFile: " + tmpFile + ". "
                        + ge.getMessage();
                    logFileHashStore.error(errMsg);
                    throw new NonMatchingChecksumException(errMsg);
                }

                String errMsg = baseErrMsg + ". tmpFile has been deleted: " + tmpFile;
                logFileHashStore.error(errMsg);
                throw new NonMatchingChecksumException(errMsg);
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
        FileHashStoreUtility.ensureNotNull(algorithm, "algorithm", "validateAlgorithm");
        FileHashStoreUtility.checkForEmptyAndValidString(algorithm, "algorithm", "validateAlgorithm");

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
     * Determines if an algorithm should be generated by checking whether the algorithm supplied is
     * included in the DefaultHashAlgorithms
     *
     * @param algorithm Algorithm to check
     * @return Boolean
     */
    protected boolean shouldCalculateAlgorithm(String algorithm) {
        FileHashStoreUtility.ensureNotNull(algorithm, "algorithm", "shouldCalculateAlgorithm");
        FileHashStoreUtility.checkForEmptyAndValidString(algorithm, "algorithm", "shouldCalculateAlgorithm");
        boolean shouldCalculateAlgorithm = true;
        for (DefaultHashAlgorithms defAlgo : DefaultHashAlgorithms.values()) {
            if (algorithm.equals(defAlgo.getName())) {
                shouldCalculateAlgorithm = false;
                break;
            }
        }
        return shouldCalculateAlgorithm;
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
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksumAlgorithm, "checksumAlgorithm", "verifyChecksumParameters");
            validateAlgorithm(checksumAlgorithm);
        }
        if (checksum != null) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksum, "checksum", "verifyChecksumParameters");
        }
        // If checksum is supplied, checksumAlgorithm cannot be empty
        if (checksum != null && !checksum.trim().isEmpty()) {
            FileHashStoreUtility.ensureNotNull(
                checksumAlgorithm, "checksumAlgorithm", "verifyChecksumParameters"
            );
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksumAlgorithm, "algorithm", "verifyChecksumParameters"
            );
        }
        // Ensure algorithm is supported, not null and not empty
        boolean requestValidation = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            requestValidation = validateAlgorithm(checksumAlgorithm);
            // Ensure checksum is not null or empty if checksumAlgorithm is supplied
            if (requestValidation) {
                FileHashStoreUtility.ensureNotNull(
                    checksum, "checksum", "verifyChecksumParameters"
                );
                FileHashStoreUtility.checkForEmptyAndValidString(
                    checksum, "checksum", "verifyChecksumParameters"
                );
            }
        }
        return requestValidation;
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap consisting of
     * algorithms and their respective hex digests. If an additional algorithm is supplied and
     * supported, it and its checksum value will be included in the hex digests map. Default
     * algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
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
            FileHashStoreUtility.checkForEmptyAndValidString(
                additionalAlgorithm, "additionalAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(additionalAlgorithm);
            generateAddAlgo = shouldCalculateAlgorithm(additionalAlgorithm);
        }
        boolean generateCsAlgo = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksumAlgorithm, "checksumAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(checksumAlgorithm);
            generateCsAlgo = shouldCalculateAlgorithm(checksumAlgorithm);
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
                "Adding additional algorithm to hex digest map, algorithm: " + additionalAlgorithm);
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (generateCsAlgo) {
            logFileHashStore.debug(
                "Adding checksum algorithm to hex digest map, algorithm: " + checksumAlgorithm);
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
            String errMsg = "Unexpected Exception ~ " + ioe.getMessage();
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
            "Object has been written to tmpFile: " + tmpFile.getName() + ". To be moved to: "
                + sha256Digest);

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
            "Moving " + entity + ", from source: " + source + ", to target: " + target);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(entity, "entity", "move");
        FileHashStoreUtility.checkForEmptyAndValidString(entity, "entity", "move");
        if (entity.equals("object") && target.exists()) {
            String errMsg = "File already exists for target: " + target;
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
                logFileHashStore.warn("Directory already exists at: " + destinationDirectoryPath
                                          + " - Skipping directory creation");
            }
        }

        // Move file
        Path sourceFilePath = source.toPath();
        Path targetFilePath = target.toPath();
        try {
            Files.move(sourceFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
            logFileHashStore.debug(
                "File moved from: " + sourceFilePath + ", to: " + targetFilePath);

        } catch (FileAlreadyExistsException faee) {
            logFileHashStore.warn(
                "File already exists, skipping request to move object. Source: " + source
                    + ". Target: " + target);

        } catch (AtomicMoveNotSupportedException amnse) {
            logFileHashStore.error("StandardCopyOption.ATOMIC_MOVE failed. AtomicMove is"
                                       + " not supported across file systems. Source: " + source
                                       + ". Target: " + target);
            throw amnse;

        } catch (IOException ioe) {
            logFileHashStore.error(
                "Unable to move file. Source: " + source + ". Target: " + target);
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
        logFileHashStore.debug("Called to delete data object with cid: " + cid);
        // Get expected path of the cid refs file & permanent address of the actual cid
        Path absCidRefsPath = getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
        String objRelativePath =
            FileHashStoreUtility.getHierarchicalPathString(DIRECTORY_DEPTH, DIRECTORY_WIDTH, cid);
        Path expectedRealPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);

        try {
            synchronizeObjectLockedCids(cid);
            if (Files.exists(absCidRefsPath)) {
                // The cid refs file exists, so the cid object cannot be deleted.
                String warnMsg = "cid refs file still contains references, skipping deletion.";
                logFileHashStore.warn(warnMsg);
            } else {
                // If file exists, delete it.
                if (Files.exists(expectedRealPath)) {
                    Files.delete(expectedRealPath);
                }
                String debugMsg = "Object deleted at" + expectedRealPath;
                logFileHashStore.debug(debugMsg);
            }
        } finally {
            // Release lock
            releaseObjectLockedCids(cid);
        }
    }

    /**
     * Create the pid refs file and create/update cid refs files in HashStore to establish
     * the relationship between a 'pid' and a 'cid'
     *
     * @param pid Persistent or authority-based identifier
     * @param cid Content identifier
     * @throws NoSuchAlgorithmException If there is an issue related to calculating hashes
     * @throws IOException If there is an issue reading/writing a refs file
     */
    protected void storeHashStoreRefsFiles(String pid, String cid) throws NoSuchAlgorithmException,
        IOException {
        Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
        Path absCidRefsPath = getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());

        if (Files.exists(absPidRefsPath) && Files.exists(absCidRefsPath)) {
            // Confirm that reference files are where they are expected to be
            verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
            // We throw an exception so the client is aware that everything is in place
            String errMsg =
                "Object with cid: " + cid + " already exists and is tagged with pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new HashStoreRefsAlreadyExistException(errMsg);

        } else if (Files.exists(absPidRefsPath) && !Files.exists(absCidRefsPath)) {
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
                    "Pid refs file exists for pid: " + pid + ", but cid refs file for: " + cid
                        + " is missing. Missing cid refs file created and tagging completed.");
                return;
            } else {
                // Check if the retrieved cid refs file exists and pid is referenced
                Path retrievedAbsCidRefsPath = getHashStoreRefsPath(
                    retrievedCid, HashStoreIdTypes.cid.getName()
                );
                if (Files.exists(retrievedAbsCidRefsPath) && isStringInRefsFile(pid, retrievedAbsCidRefsPath
                )) {
                    // This pid is accounted for and tagged as expected.
                    String errMsg = "Pid refs file already exists for pid: " + pid
                        + ", and the associated cid refs file contains the "
                        + "pid. A pid can only reference one cid.";
                    logFileHashStore.error(errMsg);
                    throw new PidRefsFileExistsException(errMsg);
                }
                // Orphaned pid refs file found, the retrieved cid refs file exists
                // but doesn't contain the pid. Proceed to overwrite the pid refs file.
            }
        } else if (!Files.exists(absPidRefsPath) && Files.exists(absCidRefsPath)) {
            // Only update cid refs file if pid is not in the file
            if (!isStringInRefsFile(pid, absCidRefsPath)) {
                updateRefsFile(pid, absCidRefsPath, "add");
            }
            // Get the pid refs file and verify tagging process
            File pidRefsTmpFile = writeRefsFile(cid, HashStoreIdTypes.pid.getName());
            File absPathPidRefsFile = absPidRefsPath.toFile();
            move(pidRefsTmpFile, absPathPidRefsFile, "refs");
            verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
            logFileHashStore.info(
                "Object with cid: " + cid + " has been updated and tagged successfully with pid: "
                    + pid);
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
        // Verify tagging process, this throws an exception if there's an issue
        verifyHashStoreRefsFiles(pid, cid, absPidRefsPath, absCidRefsPath);
        logFileHashStore.info(
            "Object with cid: " + cid + " has been tagged successfully with pid: " + pid);
    }

    /**
     * Untags a data object in HashStore by deleting the 'pid reference file' and removing the 'pid'
     * from the 'cid reference file'. This method will never delete a data object.
     *
     * @param pid Persistent or authority-based identifier
     * @param cid Content identifier of data object
     * @throws InterruptedException     When there is a synchronization issue
     * @throws NoSuchAlgorithmException When there is an algorithm used that is not supported
     * @throws IOException              When there is an issue deleting refs files
     */
    protected void unTagObject(String pid, String cid) throws InterruptedException,
        NoSuchAlgorithmException, IOException {
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(pid, "pid", "unTagObject");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "unTagObject");
        FileHashStoreUtility.ensureNotNull(cid, "cid", "unTagObject");
        FileHashStoreUtility.checkForEmptyAndValidString(cid, "cid", "unTagObject");

        Collection<Path> deleteList = new ArrayList<>();

        try {
            synchronizeObjectLockedPids(pid);
            // Before we begin untagging process, we look for the `cid` by calling
            // `findObject` which will throw custom exceptions if there is an issue with
            // the reference files, which help us determine the path to proceed with.
            try {
                Map<String, String> objInfoMap = findObject(pid);
                cid = objInfoMap.get("cid");
                // If no exceptions are thrown, we proceed to synchronization based on the `cid`
                synchronizeObjectLockedCids(cid);

                try {
                    // Get paths to reference files to work on
                    Path absCidRefsPath = getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
                    Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());

                    // Begin deletion process
                    updateRefsFile(pid, absCidRefsPath, "remove");
                    if (Files.size(absCidRefsPath) == 0) {
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                    } else {
                        String warnMsg = "Cid referenced by pid: " + pid
                            + " is not empty (refs exist for cid). Skipping object " + "deletion.";
                        logFileHashStore.warn(warnMsg);
                    }
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete all related/relevant items with the least amount of delay
                    FileHashStoreUtility.deleteListItems(deleteList);
                    logFileHashStore.info("Untagged pid: " + pid + " with cid: " + cid);

                } finally {
                    releaseObjectLockedCids(cid);
                }

            } catch (OrphanPidRefsFileException oprfe) {
                // `findObject` throws this exception when the cid refs file doesn't exist,
                // so we only need to delete the pid refs file
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                // Delete items
                FileHashStoreUtility.deleteListItems(deleteList);
                String warnMsg = "Cid refs file does not exist for pid: " + pid
                    + ". Deleted orphan pid refs file.";
                logFileHashStore.warn(warnMsg);

            } catch (OrphanRefsFilesException orfe) {
                // `findObject` throws this exception when the pid and cid refs file exists,
                // but the actual object being referenced by the pid does not exist
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                String cidRead = new String(Files.readAllBytes(absPidRefsPath));

                try {
                    // Since we must access the cid reference file, the `cid` must be synchronized
                    synchronizeObjectLockedCids(cidRead);

                    Path absCidRefsPath =
                        getHashStoreRefsPath(cidRead, HashStoreIdTypes.cid.getName());
                    updateRefsFile(pid, absCidRefsPath, "remove");
                    if (Files.size(absCidRefsPath) == 0) {
                        deleteList.add(FileHashStoreUtility.renamePathForDeletion(absCidRefsPath));
                    }
                    deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                    // Delete items
                    FileHashStoreUtility.deleteListItems(deleteList);
                    String warnMsg = "Object with cid: " + cidRead
                        + " does not exist, but pid and cid reference file found for pid: " + pid
                        + ". Deleted pid and cid ref files.";
                    logFileHashStore.warn(warnMsg);

                } finally {
                    releaseObjectLockedCids(cidRead);
                }
            } catch (PidNotFoundInCidRefsFileException pnficrfe) {
                // `findObject` throws this exception when both the pid and cid refs file exists
                // but the pid is not found in the cid refs file.

                // Rename pid refs file for deletion
                Path absPidRefsPath = getHashStoreRefsPath(pid, HashStoreIdTypes.pid.getName());
                deleteList.add(FileHashStoreUtility.renamePathForDeletion(absPidRefsPath));
                // Delete items
                FileHashStoreUtility.deleteListItems(deleteList);
                String warnMsg = "Pid not found in expected cid refs file for pid: " + pid
                    + ". Deleted orphan pid refs file.";
                logFileHashStore.warn(warnMsg);
            } catch (PidRefsFileNotFoundException prfnfe) {
                // `findObject` throws this exception if the pid refs file is not found
                // Check to see if pid is in the `cid refs file`and attempt to remove it
                Path absCidRefsPath =
                    getHashStoreRefsPath(cid, HashStoreIdTypes.cid.getName());
                if (Files.exists(absCidRefsPath) && isStringInRefsFile(pid, absCidRefsPath)) {
                    updateRefsFile(pid, absCidRefsPath, "remove");
                    String errMsg = "Pid refs file not found, removed pid found in cid refs file: "
                        + absCidRefsPath;
                    logFileHashStore.warn(errMsg);
                }
            }
        } finally {
            releaseObjectLockedPids(pid);
        }
    }

    /**
     * Verifies that the reference files for the given pid and cid exist and contain the expected
     * values.
     *
     * @param pid            Authority-based or persistent identifier
     * @param cid            Content identifier
     * @param absPidRefsPath Path to where the pid refs file exists
     * @param absCidRefsPath Path to where the cid refs file exists
     * @throws FileNotFoundException             Any refs files are missing
     * @throws CidNotFoundInPidRefsFileException When the expected cid is not found in the pid refs
     * @throws PidNotFoundInCidRefsFileException When a pid is not found in the cid refs file
     * @throws IOException                       Unable to read any of the refs files
     */
    protected void verifyHashStoreRefsFiles(
        String pid, String cid, Path absPidRefsPath, Path absCidRefsPath
    ) throws FileNotFoundException, CidNotFoundInPidRefsFileException,
        PidNotFoundInCidRefsFileException, IOException {
        // First confirm that the refs files have been created/moved to where they need to be
        if (!Files.exists(absCidRefsPath)) {
            String errMsg = "Cid refs file is missing: " + absCidRefsPath + " for pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
        if (!Files.exists(absPidRefsPath)) {
            String errMsg = "Pid refs file is missing: " + absPidRefsPath + " for cid: " + cid;
            logFileHashStore.error(errMsg);
            throw new FileNotFoundException(errMsg);
        }
        // Now confirm that the content is what is expected
        try {
            String cidRead = new String(Files.readAllBytes(absPidRefsPath));
            if (!cidRead.equals(cid)) {
                String errMsg =
                    "Unexpected cid: " + cidRead + " found in pid refs file: " + absPidRefsPath
                        + ". Expected cid: " + cid;
                logFileHashStore.error(errMsg);
                throw new CidNotFoundInPidRefsFileException(errMsg);
            }
            if (!isStringInRefsFile(pid, absCidRefsPath)) {
                String errMsg =
                    "Missing expected pid: " + pid + " in cid refs file: " + absCidRefsPath;
                logFileHashStore.error(errMsg);
                throw new PidNotFoundInCidRefsFileException(errMsg);
            }
        } catch (IOException ioe) {
            logFileHashStore.error(ioe.getMessage());
            throw ioe;
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

            logFileHashStore.debug(refType + " refs file written for: " + ref);
            return cidRefsTmpFile;

        } catch (IOException ioe) {
            String errMsg = "Unable to write refs file for ref: " + refType + " IOException: "
                + ioe.getMessage();
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
                Collection<String> lines = new ArrayList<>(Files.readAllLines(absRefsPath));

                if (updateType.equals("add")) {
                    lines.add(ref);
                    Files.write(tmpFilePath, lines, StandardOpenOption.WRITE);
                    move(tmpFile, absRefsPath.toFile(), "refs");
                    logFileHashStore.debug(
                        "Ref: " + ref + " has been added to refs file: " + absRefsPath);
                }

                if (updateType.equals("remove")) {
                    lines.remove(ref);
                    Files.write(tmpFilePath, lines, StandardOpenOption.WRITE);
                    move(tmpFile, absRefsPath.toFile(), "refs");
                    logFileHashStore.debug(
                        "Ref: " + ref + " has been removed from refs file: " + absRefsPath);
                }
            }
            // The lock is automatically released when the try block exits
        } catch (IOException ioe) {
            logFileHashStore.error(ioe.getMessage());
            throw ioe;
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
            "Writing metadata for pid: " + pid + " , with metadata namespace: " + formatId);
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(metadata, "metadata", "putMetadata");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "putMetadata");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "putMetadata");

        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = DEFAULT_METADATA_NAMESPACE;
        } else {
            FileHashStoreUtility.checkForEmptyAndValidString(formatId, "formatId", "putMetadata");
            checkedFormatId = formatId;
        }

        // Get permanent address for the given metadata document
        // All metadata documents for a pid are stored in a directory that is formed
        // by using the hash of the 'pid', with the file name being the hash of the 'pid+formatId'
        Path pathToStoredMetadata = getHashStoreMetadataPath(pid, checkedFormatId);

        File tmpMetadataFile = FileHashStoreUtility.generateTmpFile(
            "tmp", METADATA_TMP_FILE_DIRECTORY
        );
        boolean tmpMetadataWritten = writeToTmpMetadataFile(tmpMetadataFile, metadata);
        if (tmpMetadataWritten) {
            logFileHashStore.debug(
                "Tmp metadata file has been written, moving to" + " permanent location: "
                    + pathToStoredMetadata);
            File permMetadataFile = pathToStoredMetadata.toFile();
            move(tmpMetadataFile, permMetadataFile, "metadata");
        }
        logFileHashStore.debug(
            "Metadata moved successfully, permanent address: " + pathToStoredMetadata);
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
            logFileHashStore.error(ioe.getMessage());
            throw ioe;

        } finally {
            os.flush();
            os.close();
        }
    }

    /**
     * Get the absolute path to a HashStore data object
     *
     * @param abpId Authority-based or persistent identifier
     * @return Path to the HasHStore data object
     * @throws NoSuchAlgorithmException When an algorithm used to calculate a hash is not supported
     * @throws IOException Issue when reading a pid refs file to retrieve a 'cid'
     */
    protected Path getHashStoreDataObjectPath(String abpId) throws NoSuchAlgorithmException,
        IOException {
        // Retrieve the 'cid' from the pid refs file
        String objectCid;
        String hashedId = FileHashStoreUtility.getPidHexDigest(abpId, OBJECT_STORE_ALGORITHM);
        String pidRefsFileRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashedId
        );
        Path pathToPidRefsFile = REFS_PID_FILE_DIRECTORY.resolve(pidRefsFileRelativePath);
        if (!Files.exists(pathToPidRefsFile)) {
            String errMsg =
                "Pid Refs file does not exist for pid: " + abpId + " with object address: "
                    + pathToPidRefsFile + ". Cannot retrieve " + "cid.";
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        } else {
            objectCid = new String(Files.readAllBytes(pathToPidRefsFile));
        }
        // If cid is found, return the expected real path to object
        String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
        );
        // Real path to the data object
        return OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
    }

    /**
     * Get the absolute path to a HashStore metadata document
     *
     * @param abpId Authority-based or persistent identifier
     * @param formatId Metadata formatId or namespace
     * @return Path to the requested metadata document
     * @throws NoSuchAlgorithmException When an algorithm used to calculate a hash is not supported
     */
    protected Path getHashStoreMetadataPath(String abpId, String formatId)
        throws NoSuchAlgorithmException {
        // Get the pid metadata directory
        String hashedId = FileHashStoreUtility.getPidHexDigest(abpId, OBJECT_STORE_ALGORITHM);
        String pidMetadataDirRelPath = FileHashStoreUtility.getHierarchicalPathString(
            DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashedId
        );
        // The file name for the metadata document is the hash of the supplied 'pid + 'formatId'
        String metadataDocHash =
            FileHashStoreUtility.getPidHexDigest(abpId + formatId, OBJECT_STORE_ALGORITHM);
        // Real path to metadata doc
        return METADATA_STORE_DIRECTORY.resolve(pidMetadataDirRelPath).resolve(
            metadataDocHash
        );
    }

    /**
     * Get an InputStream to a metadata document if it exists in FileHashStore
     *
     * @param pid      Persistent or authority-based identifier
     * @param formatId Metadata namespace
     * @return InputStream to metadata doc
     * @throws NoSuchAlgorithmException An algorithm used in the calculation is not supported
     * @throws FileNotFoundException    If the metadata document is not found
     * @throws IOException              If there is an issue returning an input stream
     */
    protected InputStream getHashStoreMetadataInputStream(String pid, String formatId)
        throws NoSuchAlgorithmException, IOException, FileNotFoundException {
        Path metadataCidPath = getHashStoreMetadataPath(pid, formatId);

        // Check to see if metadata exists
        if (!Files.exists(metadataCidPath)) {
            String errMsg =
                "Metadata does not exist for pid: " + pid + " with formatId: " + formatId
                    + ". Metadata address: " + metadataCidPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // Return an InputStream to read from the metadata document
        try {
            InputStream metadataCidInputStream = Files.newInputStream(metadataCidPath);
            logFileHashStore.info(
                "Retrieved metadata for pid: " + pid + " with formatId: " + formatId);
            return metadataCidInputStream;

        } catch (IOException ioe) {
            String errMsg = "Unexpected error when creating InputStream for pid: " + pid
                + " with formatId: " + formatId + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);
        }
    }

    /**
     * Get the absolute path to a HashStore pid or cid ref file
     *
     * @param abpcId Authority-based identifier, persistent identifier or content identifier
     * @param refType "cid" or "pid
     * @return Path to the requested refs file
     * @throws NoSuchAlgorithmException When an algorithm used to calculate a hash is not supported
     */
    protected Path getHashStoreRefsPath(String abpcId, String refType)
        throws NoSuchAlgorithmException {
        Path realPath;
        if (refType.equalsIgnoreCase(HashStoreIdTypes.pid.getName())) {
            String hashedId = FileHashStoreUtility.getPidHexDigest(abpcId, OBJECT_STORE_ALGORITHM);
            String pidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, hashedId
            );
            realPath = REFS_PID_FILE_DIRECTORY.resolve(pidRelativePath);
        } else if (refType.equalsIgnoreCase(HashStoreIdTypes.cid.getName())) {
            String cidRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, abpcId
            );
            realPath = REFS_CID_FILE_DIRECTORY.resolve(cidRelativePath);
        } else {
            String errMsg = "formatId must be 'pid' or 'cid'";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        return realPath;
    }

    /**
     * Storing, deleting and untagging objects are synchronized together. Duplicate store object
     * requests for a pid are rejected, but deleting an object will wait for a pid to be released
     * if it's found to be in use before proceeding.
     *
     * @param pid Persistent or authority-based identifier
     * @throws InterruptedException When an issue occurs when attempting to sync the pid
     */
    private static void synchronizeObjectLockedPids(String pid)
        throws InterruptedException {
        synchronized (objectLockedPids) {
            while (objectLockedPids.contains(pid)) {
                try {
                    objectLockedPids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "Synchronization has been interrupted while trying to sync pid: " + pid;
                    logFileHashStore.warn(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug("Synchronizing objectLockedPids for pid: " + pid);
            objectLockedPids.add(pid);
        }
    }

    /**
     * Remove the given pid from 'objectLockedPids' and notify other threads
     *
     * @param pid Content identifier
     */
    private static void releaseObjectLockedPids(String pid) {
        synchronized (objectLockedPids) {
            logFileHashStore.debug("Releasing objectLockedPids for pid: " + pid);
            objectLockedPids.remove(pid);
            objectLockedPids.notify();
        }
    }

    /**
     * All requests to store/delete metadata will be accepted but must be executed serially
     *
     * @param metadataDocId Metadata document id hash(pid+formatId)
     * @throws InterruptedException When an issue occurs when attempting to sync the metadata doc
     */
    private static void synchronizeMetadataLockedDocIds(String metadataDocId)
        throws InterruptedException {
        synchronized (metadataLockedDocIds) {
            while (metadataLockedDocIds.contains(metadataDocId)) {
                try {
                    metadataLockedDocIds.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "Synchronization has been interrupted while trying to sync metadata doc: "
                            + metadataDocId;
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "Synchronizing metadataLockedDocIds for metadata doc: " + metadataDocId);
            metadataLockedDocIds.add(metadataDocId);
        }
    }

    /**
     * Remove the given metadata doc from 'metadataLockedDocIds' and notify other threads
     *
     * @param metadataDocId Metadata document id hash(pid+formatId)
     */
    private static void releaseMetadataLockedDocIds(String metadataDocId) {
        synchronized (metadataLockedDocIds) {
            logFileHashStore.debug(
                "Releasing metadataLockedDocIds for metadata doc: " + metadataDocId);
            metadataLockedDocIds.remove(metadataDocId);
            metadataLockedDocIds.notify();
        }
    }

    /**
     * Multiple threads may access a data object via its 'cid' or the respective 'cid reference
     * file' (which contains a list of 'pid's that reference a 'cid') and this needs to be
     * coordinated.
     *
     * @param cid Content identifier
     * @throws InterruptedException When an issue occurs when attempting to sync the pid
     */
    private static void synchronizeObjectLockedCids(String cid) throws InterruptedException {
        synchronized (objectLockedCids) {
            while (objectLockedCids.contains(cid)) {
                try {
                    objectLockedCids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "Synchronization has been interrupted while trying to sync cid: " + cid;
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "Synchronizing objectLockedCids for cid: " + cid);
            objectLockedCids.add(cid);
        }
    }

    /**
     * Remove the given cid from 'objectLockedCids' and notify other threads
     *
     * @param cid Content identifier
     */
    private static void releaseObjectLockedCids(String cid) {
        synchronized (objectLockedCids) {
            logFileHashStore.debug("Releasing objectLockedCids for cid: " + cid);
            objectLockedCids.remove(cid);
            objectLockedCids.notify();
        }
    }

    /**
     * Synchronize the pid tagging process since `tagObject` is a Public API method that can be
     * called directly. This is used in the scenario when the client is missing metadata but must
     * store the data object first.
     *
     * @param pid Persistent or authority-based identifier
     * @throws InterruptedException When an issue occurs when attempting to sync the pid
     */
    private static void synchronizeReferenceLockedPids(String pid) throws InterruptedException {
        synchronized (referenceLockedPids) {
            while (referenceLockedPids.contains(pid)) {
                try {
                    referenceLockedPids.wait(TIME_OUT_MILLISEC);

                } catch (InterruptedException ie) {
                    String errMsg =
                        "Synchronization has been interrupted while trying to sync pid: " + pid;
                    logFileHashStore.error(errMsg);
                    throw new InterruptedException(errMsg);
                }
            }
            logFileHashStore.debug(
                "Synchronizing referenceLockedPids for pid: " + pid);
            referenceLockedPids.add(pid);
        }
    }

    /**
     * Remove the given pid from 'referenceLockedPids' and notify other threads
     *
     * @param pid Persistent or authority-based identifier
     */
    private static void releaseReferenceLockedPids(String pid) {
        synchronized (referenceLockedPids) {
            logFileHashStore.debug("Releasing referenceLockedPids for pid: " + pid);
            referenceLockedPids.remove(pid);
            referenceLockedPids.notify();
        }
    }
}

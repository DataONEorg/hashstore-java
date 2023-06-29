package org.dataone.hashstore.filehashstore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
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
import java.util.Objects;
import java.util.Random;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import javax.xml.bind.DatatypeConverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.HashAddress;
import org.dataone.hashstore.HashStore;
import org.dataone.hashstore.exceptions.PidObjectExistsException;

/**
 * FileHashStore is a class that manages storage of objects to disk using
 * SHA-256 hex digest of an authority-based identifier as a key (usually in the
 * form of a pid). It also provides an interface for interacting with stored
 * objects and metadata.
 *
 */
public class FileHashStore implements HashStore {
    private static final Log logFileHashStore = LogFactory.getLog(FileHashStore.class);
    private static final int TIME_OUT_MILLISEC = 1000;
    private static final ArrayList<String> objectLockedIds = new ArrayList<>(100);
    private static final ArrayList<String> metadataLockedIds = new ArrayList<>(100);
    private final Path STORE_ROOT;
    private final int DIRECTORY_DEPTH;
    private final int DIRECTORY_WIDTH;
    private final String OBJECT_STORE_ALGORITHM;
    private final Path OBJECT_STORE_DIRECTORY;
    private final Path OBJECT_TMP_FILE_DIRECTORY;
    private final String METADATA_NAMESPACE;
    private final Path METADATA_STORE_DIRECTORY;
    private final Path METADATA_TMP_FILE_DIRECTORY;

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
        storeAlgorithm,
        storeMetadataNamespace
    }

    /**
     * Constructor to initialize HashStore, properties are required.
     * 
     * Note: HashStore is not responsible for ensuring that the given store path is
     * accurate. It will only check for an existing configuration, directories or
     * objects at the supplied store path before initializing.
     * 
     * Four directories will be created based on the given storePath string:
     * - .../[storePath]/objects
     * - .../[storePath]/objects/tmp
     * - .../[storePath]/metadata
     * - .../[storePath]/metadata/tmp
     * 
     * @param hashstoreProperties HashMap<String, Object> of the following keys:
     *                            storePath (Path)
     *                            storeDepth (int)
     *                            storeWidth (int)
     *                            storeAlgorithm (String)
     *                            storeMetadataNamespace (String)
     * @throws IllegalArgumentException Constructor arguments cannot be null, empty
     *                                  or less than 0
     * @throws IOException              Issue with creating directories
     * @throws NoSuchAlgorithmException Unsupported store algorithm
     */
    public FileHashStore(HashMap<String, Object> hashstoreProperties)
            throws IllegalArgumentException, IOException, NoSuchAlgorithmException {
        if (hashstoreProperties == null) {
            String errMsg = "FileHashStore - Properties cannot be null.";
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Get properties
        Path storePath = (Path) hashstoreProperties.get(HashStoreProperties.storePath.name());
        int storeDepth = (int) hashstoreProperties.get(HashStoreProperties.storeDepth.name());
        int storeWidth = (int) hashstoreProperties.get(HashStoreProperties.storeWidth.name());
        String storeAlgorithm = (String) hashstoreProperties.get(HashStoreProperties.storeAlgorithm.name());
        String storeMetadataNamespace = (String) hashstoreProperties
                .get(HashStoreProperties.storeMetadataNamespace.name());

        // Validate input parameters
        if (storePath == null) {
            String errMsg = "FileHashStore - storePath cannot be null.";
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        if (storeDepth <= 0 || storeWidth <= 0) {
            String errMsg = "FileHashStore - Depth and width must be greater than 0. Depth: " + storeDepth
                    + ". Width: " + storeWidth;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Ensure algorithm supplied is not empty, not null and supported
        this.validateAlgorithm(storeAlgorithm);

        if (storeMetadataNamespace == null || storeMetadataNamespace.trim().isEmpty()) {
            String errMsg = "FileHashStore - Store metadata namespace (formatId) cannot be null or empty. Namespace: "
                    + storeMetadataNamespace;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Check to see if configuration exists before initializing
        Path hashstoreYamlPredictedPath = Paths.get(storePath + "/hashstore.yaml");
        if (Files.exists(hashstoreYamlPredictedPath)) {
            logFileHashStore.debug("FileHashStore - 'hashstore.yaml' found, verifying properties.");

            HashMap<String, Object> hsProperties = this.getHashStoreYaml(storePath);
            Path existingStorePath = (Path) hsProperties.get("storePath");
            int existingStoreDepth = (int) hsProperties.get("storeDepth");
            int existingStoreWidth = (int) hsProperties.get("storeWidth");
            String existingStoreAlgorithm = (String) hsProperties.get("storeAlgorithm");
            String existingStoreMetadataNs = (String) hsProperties.get("storeMetadataNamespace");

            // Verify properties when 'hashstore.yaml' found
            checkConfigurationEquality("store path", storePath, existingStorePath);
            checkConfigurationEquality("store depth", storeDepth, existingStoreDepth);
            checkConfigurationEquality("store width", storeWidth, existingStoreWidth);
            checkConfigurationEquality("store algorithm", storeAlgorithm, existingStoreAlgorithm);
            checkConfigurationEquality("store algorithm", storeMetadataNamespace, existingStoreMetadataNs);

        } else {
            // Check if HashStore exists at the given store path (and is missing config)
            logFileHashStore
                    .debug("FileHashStore - 'hashstore.yaml' not found, check store path for objects and directories.");

            if (Files.isDirectory(storePath)) {
                File[] storePathFileList = storePath.toFile().listFiles();
                if (storePathFileList == null || storePathFileList.length > 0) {
                    String errMsg = "FileHashStore - Missing 'hashstore.yaml' but directories and/or objects found.";
                    logFileHashStore.fatal(errMsg);
                    throw new IllegalStateException(errMsg);
                }
            }
            logFileHashStore.debug("FileHashStore - 'hashstore.yaml' not found and store path not yet initialized.");
        }

        // HashStore configuration has been checked, proceed with initialization
        this.STORE_ROOT = storePath;
        this.DIRECTORY_DEPTH = storeDepth;
        this.DIRECTORY_WIDTH = storeWidth;
        this.OBJECT_STORE_ALGORITHM = storeAlgorithm;
        this.METADATA_NAMESPACE = storeMetadataNamespace;
        // Resolve object/metadata directories
        this.OBJECT_STORE_DIRECTORY = storePath.resolve("objects");
        this.METADATA_STORE_DIRECTORY = storePath.resolve("metadata");
        // Resolve tmp object/metadata directory paths
        this.OBJECT_TMP_FILE_DIRECTORY = this.OBJECT_STORE_DIRECTORY.resolve("tmp");
        this.METADATA_TMP_FILE_DIRECTORY = this.METADATA_STORE_DIRECTORY.resolve("tmp");
        try {
            // Physically create object & metadata store and tmp directories
            Files.createDirectories(this.OBJECT_STORE_DIRECTORY);
            Files.createDirectories(this.METADATA_STORE_DIRECTORY);
            Files.createDirectories(this.OBJECT_TMP_FILE_DIRECTORY);
            Files.createDirectories(this.METADATA_TMP_FILE_DIRECTORY);
            logFileHashStore.debug("FileHashStore - Created store and store tmp directories.");

        } catch (IOException ioe) {
            logFileHashStore.fatal(
                    "FileHashStore - Failed to initialize FileHashStore - unable to create directories. Exception: "
                            + ioe.getMessage());
            throw ioe;
        }
        logFileHashStore
                .debug("FileHashStore - HashStore initialized. Store Depth: " + this.DIRECTORY_DEPTH + ". Store Width: "
                        + this.DIRECTORY_WIDTH + ". Store Algorithm: " + this.OBJECT_STORE_ALGORITHM
                        + ". Store Metadata Namespace: " + this.METADATA_NAMESPACE);

        // Write configuration file 'hashstore.yaml' to store root
        Path hashstoreYaml = this.STORE_ROOT.resolve("hashstore.yaml");
        if (!Files.exists(hashstoreYaml)) {
            String hashstoreYamlContent = FileHashStore.buildHashStoreYamlString(this.STORE_ROOT, this.DIRECTORY_DEPTH,
                    this.DIRECTORY_WIDTH, this.OBJECT_STORE_ALGORITHM, this.METADATA_NAMESPACE);
            this.putHashStoreYaml(hashstoreYamlContent);
            logFileHashStore.info("FileHashStore - 'hashstore.yaml' written to storePath: " + hashstoreYaml);
        } else {
            logFileHashStore.info(
                    "FileHashStore - 'hashstore.yaml' exists and has been verified. Initializing FileHashStore.");
        }
    }

    // Configuration Methods

    /**
     * Get the properties of HashStore from 'hashstore.yaml'
     *
     * @param storePath Path to root of store
     * @return HashMap of the properties
     * @throws IOException If `hashstore.yaml` doesn't exist
     */
    protected HashMap<String, Object> getHashStoreYaml(Path storePath) throws IOException {
        Path hashstoreYaml = storePath.resolve("hashstore.yaml");
        File hashStoreYaml = hashstoreYaml.toFile();
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        HashMap<String, Object> hsProperties = new HashMap<>();

        try {
            HashMap<?, ?> hashStoreYamlProperties = om.readValue(hashStoreYaml, HashMap.class);
            String yamlStorePath = (String) hashStoreYamlProperties.get("store_path");
            hsProperties.put("storePath", Paths.get(yamlStorePath));
            hsProperties.put("storeDepth", hashStoreYamlProperties.get("store_depth"));
            hsProperties.put("storeWidth", hashStoreYamlProperties.get("store_width"));
            hsProperties.put("storeAlgorithm", hashStoreYamlProperties.get("store_algorithm"));
            hsProperties.put("storeMetadataNamespace", hashStoreYamlProperties.get("store_metadata_namespace"));

        } catch (IOException ioe) {
            logFileHashStore
                    .fatal("FileHashStore.getHashStoreYaml() - Unable to retrieve 'hashstore.yaml'. IOException: "
                            + ioe.getMessage());
            throw ioe;
        }

        return hsProperties;
    }

    /**
     * Write a 'hashstore.yaml' file to this.STORE_ROOT
     * 
     * @param yamlString Content of the HashStore configuration
     * @throws IOException If unable to write `hashstore.yaml`
     */
    protected void putHashStoreYaml(String yamlString) throws IOException {
        Path hashstoreYaml = this.STORE_ROOT.resolve("hashstore.yaml");

        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(Files.newOutputStream(hashstoreYaml), StandardCharsets.UTF_8))) {
            writer.write(yamlString);

        } catch (IOException ioe) {
            logFileHashStore
                    .fatal("FileHashStore.writeHashStoreYaml() - Unable to write 'hashstore.yaml'. IOException: "
                            + ioe.getMessage());
            throw ioe;
        }
    }

    /**
     * Checks the equality of a supplied value with an existing value for a specific
     * configuration property.
     *
     * @param propertyName  The name of the config property being checked
     * @param suppliedValue The value supplied for the config property
     * @param existingValue The existing value of the config property
     * @throws IllegalArgumentException If the supplied value is not equal to the
     *                                  existing value
     */
    protected void checkConfigurationEquality(String propertyName, Object suppliedValue, Object existingValue) {
        if (!Objects.equals(suppliedValue, existingValue)) {
            String errMsg = "FileHashStore.checkConfigurationEquality() - Supplied " + propertyName + ": "
                    + suppliedValue + " does not match the existing configuration value: " + existingValue;
            logFileHashStore.fatal(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Build the string content of the configuration file for HashStore -
     * 'hashstore.yaml'
     * 
     * @param storePath              Root path of store
     * @param storeDepth             Depth of store
     * @param storeWidth             Width of store
     * @param storeAlgorithm         Algorithm to use to calculate the hex digest
     *                               for the
     *                               permanent address of a data sobject
     * @param storeMetadataNamespace default formatId of hashstore metadata
     * @return String that representing the contents of 'hashstore.yaml'
     */
    protected static String buildHashStoreYamlString(Path storePath, int storeDepth, int storeWidth,
            String storeAlgorithm, String storeMetadataNamespace) {
        return String.format(
                "# Default configuration variables for HashStore\n\n" +
                        "############### Store Path ###############\n" +
                        "# Default path for `FileHashStore` if no path is provided\n" +
                        "store_path: \"%s\"\n\n" +
                        "############### Directory Structure ###############\n" +
                        "# Desired amount of directories when sharding an object to form the permanent address\n" +
                        "store_depth: %d  # WARNING: DO NOT CHANGE UNLESS SETTING UP NEW HASHSTORE\n" +
                        "# Width of directories created when sharding an object to form the permanent address\n" +
                        "store_width: %d  # WARNING: DO NOT CHANGE UNLESS SETTING UP NEW HASHSTORE\n" +
                        "# Example:\n" +
                        "# Below, objects are shown listed in directories that are # levels deep (DIR_DEPTH=3),\n" +
                        "# with each directory consisting of 2 characters (DIR_WIDTH=2).\n" +
                        "#    /var/filehashstore/objects\n" +
                        "#    ├── 7f\n" +
                        "#    │   └── 5c\n" +
                        "#    │       └── c1\n" +
                        "#    │           └── 8f0b04e812a3b4c8f686ce34e6fec558804bf61e54b176742a7f6368d6\n\n" +
                        "############### Format of the Metadata ###############\n" +
                        "store_sysmeta_namespace: \"http://ns.dataone.org/service/types/v2.0\"\n\n" +
                        "############### Hash Algorithms ###############\n" +
                        "# Hash algorithm to use when calculating object's hex digest for the permanent address\n" +
                        "store_algorithm: \"%s\"\n" +
                        "############### Hash Algorithms ###############\n" +
                        "# Hash algorithm to use when calculating object's hex digest for the permanent address\n" +
                        "store_metadata_namespace: \"%s\"\n" +
                        "# Algorithm values supported by python hashlib 3.9.0+ for File Hash Store (FHS)\n" +
                        "# The default algorithm list includes the hash algorithms calculated when storing an\n" +
                        "# object to disk and returned to the caller after successful storage.\n" +
                        "store_default_algo_list:\n" +
                        "- \"MD5\"\n" +
                        "- \"SHA-1\"\n" +
                        "- \"SHA-256\"\n" +
                        "- \"SHA-384\"\n" +
                        "- \"SHA-512\"\n",
                storePath, storeDepth, storeWidth, storeAlgorithm, storeMetadataNamespace);
    }

    // Public API Methods

    @Override
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException {
        logFileHashStore.debug("FileHashStore.storeObject - Called to store object for pid: " + pid);

        // Begin input validation
        if (object == null) {
            String errMsg = "FileHashStore.storeObject - InputStream cannot be null, request for storing pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.storeObject - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
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
                String errMsg = "FileHashStore.storeObject - Duplicate object request encountered for pid: " + pid
                        + ". Already in progress.";
                logFileHashStore.warn(errMsg);
                throw new RuntimeException(errMsg);
            }
            logFileHashStore.debug("FileHashStore.storeObject - Synchronizing objectLockedIds for pid: " + pid);
            objectLockedIds.add(pid);
        }

        try {
            logFileHashStore.debug("FileHashStore.storeObject - .putObject() request for pid: " + pid
                    + ". additionalAlgorithm: " + additionalAlgorithm + ". checksum: " + checksum
                    + ". checksumAlgorithm: " + checksumAlgorithm);
            // Store object
            HashAddress objInfo = this.putObject(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
            logFileHashStore.info(
                    "FileHashStore.storeObject - Object stored for pid: " + pid + ". Permanent address: "
                            + objInfo.getAbsPath());
            return objInfo;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "FileHashStore.storeObject - Unable to store object for pid: " + pid
                    + ". NoSuchAlgorithmException: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } catch (PidObjectExistsException poee) {
            String errMsg = "FileHashStore.storeObject - Unable to store object for pid: " + pid
                    + ". PidObjectExistsException: " + poee.getMessage();
            logFileHashStore.error(errMsg);
            throw poee;

        } catch (IOException ioe) {
            // Covers AtomicMoveNotSupportedException, FileNotFoundException
            String errMsg = "FileHashStore.storeObject - Unable to store object for pid: " + pid
                    + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (RuntimeException re) {
            // Covers SecurityException, IllegalArgumentException, NullPointerException
            String errMsg = "FileHashStore.storeObject - Unable to store object for pid: " + pid
                    + ". Runtime Exception: " + re.getMessage();
            logFileHashStore.error(errMsg);
            throw re;

        } finally {
            // Release lock
            synchronized (objectLockedIds) {
                logFileHashStore.debug("FileHashStore.storeObject - Releasing objectLockedIds for pid: " + pid);
                objectLockedIds.remove(pid);
                objectLockedIds.notifyAll();
            }
        }
    }

    @Override
    public String storeMetadata(InputStream metadata, String pid, String formatId)
            throws IOException, FileNotFoundException, IllegalArgumentException, InterruptedException,
            NoSuchAlgorithmException {
        logFileHashStore.debug("FileHashStore.storeMetadata - Called to store metadata for pid: " + pid
                + ", with formatId: " + formatId);

        // Begin input validation
        if (metadata == null) {
            String errMsg = "FileHashStore.storeMetadata - InputStream cannot be null, request for storing pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.storeMetadata - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Determine metadata namespace
        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = this.METADATA_NAMESPACE;
        } else if (formatId.trim().isEmpty()) {
            String errMsg = "FileHashStore.storeMetadata - formatId (metadata namespace) cannot be empty, it must be"
                    + " supplied or null for default store namespace.";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        } else {
            checkedFormatId = formatId;
        }

        // Lock pid for thread safety, transaction control and atomic writing
        // Metadata storage requests for the same pid must be written serially
        synchronized (metadataLockedIds) {
            while (metadataLockedIds.contains(pid)) {
                try {
                    metadataLockedIds.wait(TIME_OUT_MILLISEC);
                } catch (InterruptedException ie) {
                    String errMsg = "FileHashStore.storeMetadata - Metadata lock was interrupted while storing metadata for: "
                            + pid + ". InterruptedException: " + ie.getMessage();
                    logFileHashStore.warn(errMsg);
                    throw ie;
                }
            }
            logFileHashStore.debug("FileHashStore.storeMetadata - Synchronizing metadataLockedIds for pid: " + pid);
            metadataLockedIds.add(pid);
        }

        try {
            logFileHashStore.debug("FileHashStore.storeMetadata - .putMetadata() request for pid: " + pid
                    + ". formatId: " + checkedFormatId);
            // Store metadata
            String metadataCid = this.putMetadata(metadata, pid, checkedFormatId);
            logFileHashStore.info(
                    "FileHashStore.storeMetadata - Metadata stored for pid: " + pid
                            + ". Metadata Content Identifier (metadataCid): " + metadataCid);
            return metadataCid;

        } catch (IOException ioe) {
            // Covers FileNotFoundException
            String errMsg = "FileHashStore.storeMetadata - Unable to store metadata, IOException encountered: "
                    + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "FileHashStore.storeMetadata - Unable to store metadata, algorithm to calculate"
                    + " permanent address is not supported: " + nsae.getMessage();
            logFileHashStore.error(errMsg);
            throw nsae;

        } finally {
            // Release lock
            synchronized (metadataLockedIds) {
                logFileHashStore.debug("FileHashStore.storeMetadata - Releasing metadataLockedIds for pid: " + pid);
                metadataLockedIds.remove(pid);
                metadataLockedIds.notifyAll();
            }
        }
    }

    @Override
    public InputStream retrieveObject(String pid)
            throws IllegalArgumentException, NoSuchAlgorithmException, FileNotFoundException, IOException {
        logFileHashStore.debug("FileHashStore.retrieveObject - Called to retrieve object for pid: " + pid);

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.retrieveObject - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Get permanent address of the pid by calculating its sha-256 hex digest
        String objectCid = this.getPidHexDigest(pid, OBJECT_STORE_ALGORITHM);
        String objShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                objectCid);
        Path objHashAddressPath = this.OBJECT_STORE_DIRECTORY.resolve(objShardString);

        // Check to see if object exists
        if (!Files.exists(objHashAddressPath)) {
            String errMsg = "FileHashStore.retrieveObject - File does not exist for pid: " + pid
                    + " with object address: " + objHashAddressPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // If so, return an input stream for the object
        try {
            InputStream objectCidInputStream = Files.newInputStream(objHashAddressPath);
            logFileHashStore.info("FileHashStore.retrieveObject - Retrieved object for pid: " + pid);
            return objectCidInputStream;

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.retrieveObject - Unexpected error when creating InputStream for pid: "
                    + pid + ", IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);

        }

    }

    @Override
    public InputStream retrieveMetadata(String pid, String formatId) throws Exception {
        logFileHashStore.debug("FileHashStore.retrieveMetadata - Called to retrieve metadata for pid: " + pid
                + " with formatId: " + formatId);

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.retrieveMetadata - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }
        if (formatId == null || formatId.trim().isEmpty()) {
            String errMsg = "FileHashStore.retrieveMetadata - formatId cannot be null or empty, formatId: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Get permanent address of the pid by calculating its sha-256 hex digest
        String metadataCid = this.getPidHexDigest(pid + formatId, OBJECT_STORE_ALGORITHM);
        String metadataShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                metadataCid);
        Path metadataHashAddressPath = this.METADATA_STORE_DIRECTORY.resolve(metadataShardString);

        // Check to see if metadata exists
        if (!Files.exists(metadataHashAddressPath)) {
            String errMsg = "FileHashStore.retrieveMetadata - Metadata does not exist for pid: " + pid
                    + " with formatId: " + formatId + ". Metadata address: " + metadataHashAddressPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // If so, return an input stream for the metadata
        try {
            InputStream metadataCidInputStream = Files.newInputStream(metadataHashAddressPath);
            logFileHashStore.info("FileHashStore.retrieveMetadata - Retrieved metadata for pid: " + pid
                    + " with formatId: " + formatId);
            return metadataCidInputStream;

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.retrieveMetadata - Unexpected error when creating InputStream for pid: "
                    + pid + " with formatId: " + formatId + ". IOException: " + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);

        }
    }

    @Override
    public boolean deleteObject(String pid)
            throws IllegalArgumentException, FileNotFoundException, IOException, NoSuchAlgorithmException {
        logFileHashStore.debug("FileHashStore.deleteObject - Called to delete object for pid: " + pid);

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.deleteObject - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Get permanent address of the pid by calculating its sha-256 hex digest
        String objectCid = this.getPidHexDigest(pid, OBJECT_STORE_ALGORITHM);
        String objShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                objectCid);
        Path objHashAddressPath = this.OBJECT_STORE_DIRECTORY.resolve(objShardString);

        // Check to see if object exists
        if (!Files.exists(objHashAddressPath)) {
            String errMsg = "FileHashStore.deleteObject - File does not exist for pid: " + pid
                    + " with object address: " + objHashAddressPath;
            logFileHashStore.warn(errMsg);
            throw new FileNotFoundException(errMsg);
        }

        // Delete file
        Files.delete(objHashAddressPath);

        // Then delete any empty directories
        Path parent = objHashAddressPath.getParent();
        while (parent != null && isDirectoryEmpty(parent)) {
            if (parent.equals(this.OBJECT_STORE_DIRECTORY)) {
                // Do not delete the object store directory
                break;

            } else {
                Files.delete(parent);
                logFileHashStore.info("FileHashStore.deleteObject - Deleting parent directory for: " + pid
                        + " with parent address: " + parent);
                parent = parent.getParent();
            }
        }

        logFileHashStore.info("FileHashStore.deleteObject - File deleted for: " + pid + " with object address: "
                + objHashAddressPath);
        return true;
    }

    @Override
    public boolean deleteMetadata(String pid, String formatId) throws Exception {
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
     * @param object              Inputstream for file
     * @param pid                 Authority-based identifier
     * @param additionalAlgorithm Optional checksum value to generate in hex digests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
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
     * @throws PidObjectExistsException        Duplicate object in store exists
     * @throws IllegalArgumentException        When signature values are empty
     *                                         (checksum, pid, etc.)
     * @throws NullPointerException            Arguments are null for pid or object
     * @throws AtomicMoveNotSupportedException When attempting to move files across
     *                                         file systems
     */
    protected HashAddress putObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws IOException, NoSuchAlgorithmException, SecurityException, FileNotFoundException,
            PidObjectExistsException, IllegalArgumentException, NullPointerException,
            AtomicMoveNotSupportedException {
        logFileHashStore.debug("FileHashStore.putObject - Called to put object for pid: " + pid);

        // Begin input validation
        if (object == null) {
            String errMsg = "FileHashStore.putObject - InputStream cannot be null, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        // pid cannot be empty or null
        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.putObject - pid cannot be null or empty. pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
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
        String objectCid = this.getPidHexDigest(pid, this.OBJECT_STORE_ALGORITHM);
        String objShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                objectCid);
        Path objHashAddressPath = this.OBJECT_STORE_DIRECTORY.resolve(objShardString);

        // If file (pid hash) exists, reject request immediately
        if (Files.exists(objHashAddressPath)) {
            String errMsg = "FileHashStore.putObject - File already exists for pid: " + pid
                    + ". Object address: " + objHashAddressPath + ". Aborting request.";
            logFileHashStore.warn(errMsg);
            throw new PidObjectExistsException(errMsg);
        }

        // Generate tmp file and write to it
        logFileHashStore.debug("FileHashStore.putObject - Generating tmpFile");
        File tmpFile = this.generateTmpFile("tmp", this.OBJECT_TMP_FILE_DIRECTORY);
        Map<String, String> hexDigests = this.writeToTmpFileAndGenerateChecksums(tmpFile, object,
                additionalAlgorithm, checksumAlgorithm);

        // Validate object if checksum and checksum algorithm is passed
        if (requestValidation) {
            logFileHashStore
                    .info("FileHashStore.putObject - Validating object, checksum arguments supplied and valid.");
            String digestFromHexDigests = hexDigests.get(checksumAlgorithm);
            if (digestFromHexDigests == null) {
                String errMsg = "FileHashStore.putObject - checksum not found in hex digest map when validating object."
                        + " checksumAlgorithm checked: " + checksumAlgorithm;
                logFileHashStore.error(errMsg);
                throw new NoSuchAlgorithmException(errMsg);
            }

            if (!checksum.equals(digestFromHexDigests)) {
                // Delete tmp File
                boolean deleteStatus = tmpFile.delete();
                if (!deleteStatus) {
                    String errMsg = "FileHashStore.putObject - Object cannot be validated, failed to delete tmpFile: "
                            + tmpFile.getName();
                    logFileHashStore.error(errMsg);
                    throw new IOException(errMsg);
                }
                String errMsg = "FileHashStore.putObject - Checksum given is not equal to the calculated hex digest: "
                        + digestFromHexDigests + ". Checksum provided: " + checksum + ". Deleting tmpFile: "
                        + tmpFile.getName();
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }

        // Move object
        boolean isDuplicate = true;
        logFileHashStore.debug("FileHashStore.putObject - Moving object: " + tmpFile.toString() + ". Destination: "
                + objHashAddressPath);
        if (Files.exists(objHashAddressPath)) {
            boolean deleteStatus = tmpFile.delete();
            if (!deleteStatus) {
                String errMsg = "FileHashStore.putObject - Object is found to be a duplicate after writing tmpFile."
                        + " Attempted to delete tmpFile but failed: " + tmpFile.getName();
                logFileHashStore.error(errMsg);
                throw new IOException(errMsg);
            }

            objectCid = null;
            objShardString = null;
            objHashAddressPath = null;
            logFileHashStore.info(
                    "FileHashStore.putObject - Did not move object, duplicate file found for pid: " + pid
                            + ". Deleted tmpFile: " + tmpFile.getName());
        } else {
            File permFile = objHashAddressPath.toFile();
            boolean hasMoved = this.move(tmpFile, permFile, "object");
            if (hasMoved) {
                isDuplicate = false;
            }
            logFileHashStore
                    .debug("FileHashStore.putObject - Move object success, permanent address: " + objHashAddressPath);
        }

        // Create HashAddress object to return with pertinent data
        return new HashAddress(objectCid, objShardString, objHashAddressPath, isDuplicate,
                hexDigests);
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
    protected boolean validateAlgorithm(String algorithm)
            throws NullPointerException, IllegalArgumentException, NoSuchAlgorithmException {
        if (algorithm == null) {
            String errMsg = "FileHashStore.validateAlgorithm - algorithm is null.";
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        if (algorithm.trim().isEmpty()) {
            String errMsg = "FileHashStore.validateAlgorithm - algorithm is empty.";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

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
                String errMsg = "FileHashStore.verifyChecksumParameters - checksumAlgorithm is null.";
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }

            if (checksumAlgorithm.trim().isEmpty()) {
                String errMsg = "FileHashStore.verifyChecksumParameters - checksumAlgorithm is empty.";
                logFileHashStore.error(errMsg);
                throw new IllegalArgumentException(errMsg);
            }
        }
        // Ensure algorithm is supported, not null and not empty
        boolean requestValidation = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.trim().isEmpty()) {
            requestValidation = this.validateAlgorithm(checksumAlgorithm);
            // Ensure checksum is not null or empty if checksumAlgorithm is supplied in
            if (requestValidation) {
                if (checksum == null) {
                    String errMsg = "FileHashStore.verifyChecksumParameters - checksum is null.";
                    logFileHashStore.error(errMsg);
                    throw new NullPointerException(errMsg);
                }

                if (checksum.trim().isEmpty()) {
                    String errMsg = "FileHashStore.verifyChecksumParameters - checksum is empty.";
                    logFileHashStore.error(errMsg);
                    throw new IllegalArgumentException(errMsg);
                }
            }
        }
        return requestValidation;
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
            throw new IllegalArgumentException("Algorithm cannot be null or empty");
        }

        if (pid == null || pid.trim().isEmpty()) {
            throw new IllegalArgumentException("String cannot be null or empty");
        }

        boolean algorithmSupported = this.validateAlgorithm(algorithm);
        if (!algorithmSupported) {
            String errMsg = "Algorithm not supported. Supported algorithms: "
                    + Arrays.toString(SUPPORTED_HASH_ALGORITHMS);
            throw new NoSuchAlgorithmException(errMsg);
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
            logFileHashStore.trace("FileHashStore.generateTmpFile - tmpFile generated: " + newFile.getAbsolutePath());
            return newFile;

        } catch (IOException ioe) {
            String errMsg = "FileHashStore.generateTmpFile - Unable to generate tmpFile, IOException: "
                    + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw new IOException(errMsg);

        } catch (SecurityException se) {
            String errMsg = "FileHashStore.generateTmpFile - Unable to generate tmpFile, SecurityException: "
                    + se.getMessage();
            logFileHashStore.error(errMsg);
            throw new SecurityException(errMsg);

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
     * @param checksumAlgorithm   checksum algorithm to calculate hex digest for
     *                            to verifying object
     * 
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException Unable to generate new instance of supplied
     *                                  algorithm
     * @throws IOException              Issue with writing file from InputStream
     * @throws SecurityException        Unable to write to tmpFile
     * @throws FileNotFoundException    tmnpFile cannot be found
     */
    protected Map<String, String> writeToTmpFileAndGenerateChecksums(File tmpFile, InputStream dataStream,
            String additionalAlgorithm, String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, FileNotFoundException, SecurityException {
        if (additionalAlgorithm != null) {
            if (additionalAlgorithm.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Additional algorithm cannot be empty");
            }
            this.validateAlgorithm(additionalAlgorithm);
        }
        if (checksumAlgorithm != null) {
            if (checksumAlgorithm.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "Additional algorithm cannot be empty");
            }
            this.validateAlgorithm(checksumAlgorithm);
        }

        MessageDigest additionalAlgo = null;
        MessageDigest checksumAlgo = null;
        Map<String, String> hexDigests = new HashMap<>();

        FileOutputStream os = new FileOutputStream(tmpFile);
        // Replace `_` with `-` in order to get valid MessageDigest objects
        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.name());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.name().replace("_", "-"));
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.name().replace("_", "-"));
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.name().replace("_", "-"));
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.name().replace("_", "-"));
        if (additionalAlgorithm != null) {
            logFileHashStore.debug(
                    "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding additional algorithm to hex digest map,"
                            + " algorithm: " + additionalAlgorithm);
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
            logFileHashStore.debug(
                    "FileHashStore.writeToTmpFileAndGenerateChecksums - Adding checksum algorithm to hex digest map,"
                            + " algorithm: " + checksumAlgorithm);
            checksumAlgo = MessageDigest.getInstance(checksumAlgorithm);
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
                    additionalAlgo.update(buffer, 0, bytesRead);
                }
                if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
                    checksumAlgo.update(buffer, 0, bytesRead);
                }
            }
        } catch (IOException ioe) {
            String errMsg = "FileHashStore.writeToTmpFileAndGenerateChecksums - Unexpected IOException encountered: "
                    + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;
        } finally {
            os.flush();
            os.close();
        }

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
            String extraAlgoDigest = DatatypeConverter.printHexBinary(additionalAlgo.digest()).toLowerCase();
            hexDigests.put(additionalAlgorithm, extraAlgoDigest);
        }
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
            String extraChecksumDigest = DatatypeConverter.printHexBinary(checksumAlgo.digest()).toLowerCase();
            hexDigests.put(checksumAlgorithm, extraChecksumDigest);
        }
        logFileHashStore.debug("FileHashStore.writeToTmpFileAndGenerateChecksums - Object has been written to tmpFile: "
                + tmpFile.getName() + ". To be moved to: " + sha256Digest);

        return hexDigests;
    }

    /**
     * Moves an object from one location to another. When the "entity" given is
     * "object", if the target already exists, it will throw an exception to prevent
     * the target object to be overwritten. Data "object" files are stored once and
     * only once.
     * 
     * @param source File to move
     * @param target Where to move the file
     * @param entity Type of object to move
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
    protected boolean move(File source, File target, String entity)
            throws IOException, SecurityException, AtomicMoveNotSupportedException, FileAlreadyExistsException {
        logFileHashStore.debug("FileHashStore.move - called to move entity type: " + entity + ", from source: "
                + source + ", to target: " + target);
        // Validate input parameters
        if (entity == null) {
            String errMsg = "FileHashStore.move - entity cannot be null, must be 'object' for storeObject() or"
                    + " 'metadata' for storeMetadata()";
            logFileHashStore.debug(errMsg);
            throw new NullPointerException(errMsg);

        } else if (entity.trim().isEmpty()) {
            String errMsg = "FileHashStore.move - entity cannot be empty, must be 'object' for storeObject()";
            logFileHashStore.debug(errMsg);
            throw new IllegalArgumentException(errMsg);

        } else if (entity.equals("object") && target.exists()) {
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
            logFileHashStore
                    .debug("FileHashStore.move - file moved from: " + sourceFilePath + ", to: " + targetFilePath);
            return true;

        } catch (AtomicMoveNotSupportedException amnse) {
            logFileHashStore.error(
                    "FileHashStore.move - StandardCopyOption.ATOMIC_MOVE failed. AtomicMove is not supported across"
                            + " file systems. Source: " + source + ". Target: " + target);
            throw amnse;

        } catch (IOException ioe) {
            logFileHashStore.error("FileHashStore.move - Unable to move file. Source: " + source
                    + ". Target: " + target);
            throw ioe;

        }
    }

    /**
     * Takes a given input stream and writes it to its permanent address on disk
     * based on the SHA-256 hex digest of the given pid + formatId. If no formatId
     * is supplied, it will use the default store namespace as defined by
     * `hashstore.yaml`
     * 
     * @param metadata Inputstream to metadata
     * @param pid      Authority-based identifier
     * @param formatId Metadata formatId or namespace
     * @return Metadata content identifier
     * @throws NoSuchAlgorithmException When the algorithm used to calculate
     *                                  permanent address is not supported
     * @throws IOException              I/O error when writing to tmp file
     */
    protected String putMetadata(InputStream metadata, String pid, String formatId)
            throws NoSuchAlgorithmException, IOException {
        logFileHashStore.debug(
                "FileHashStore.putMetadata - Called to put metadata for pid:" + pid + " , with metadata namespace: "
                        + formatId);

        // Begin input validation
        if (metadata == null) {
            String errMsg = "FileHashStore.putMetadata - InputStream cannot be null, request for storing pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new NullPointerException(errMsg);
        }

        if (pid == null || pid.trim().isEmpty()) {
            String errMsg = "FileHashStore.putMetadata - pid cannot be null or empty, pid: " + pid;
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        }

        // Determine metadata namespace
        // If no formatId is supplied, use the default namespace to store metadata
        String checkedFormatId;
        if (formatId == null) {
            checkedFormatId = this.METADATA_NAMESPACE;
        } else if (formatId.trim().isEmpty()) {
            String errMsg = "FileHashStore.putMetadata - formatId (metadata namespace) cannot be empty, it must"
                    + " be supplied, or null for the default store namespace.";
            logFileHashStore.error(errMsg);
            throw new IllegalArgumentException(errMsg);
        } else {
            checkedFormatId = formatId;
        }

        // Get permanent address for the given metadata document
        String metadataCid = this.getPidHexDigest(pid + checkedFormatId, this.OBJECT_STORE_ALGORITHM);
        String metadataCidShardString = this.getHierarchicalPathString(this.DIRECTORY_DEPTH, this.DIRECTORY_WIDTH,
                metadataCid);
        Path metadataCidAbsPath = this.METADATA_STORE_DIRECTORY.resolve(metadataCidShardString);

        // Store metadata to tmpMetadataFile
        File tmpMetadataFile = this.generateTmpFile("tmp", this.METADATA_TMP_FILE_DIRECTORY);
        boolean tmpMetadataWritten = this.writeToTmpMetadataFile(tmpMetadataFile, metadata);
        if (tmpMetadataWritten) {
            logFileHashStore.debug(
                    "FileHashStore.putObject - tmp metadata file has been written, moving to permanent location: "
                            + metadataCidAbsPath);
            File permMetadataFile = metadataCidAbsPath.toFile();
            this.move(tmpMetadataFile, permMetadataFile, "metadata");
        }
        logFileHashStore
                .info("FileHashStore.putObject - Move metadata success, permanent address: " + metadataCidAbsPath);
        return metadataCid;
    }

    /**
     * Write the supplied metadata content into the given tmpFile
     * 
     * @param tmpFile        File to write into
     * @param metadataStream Stream of metadata content
     * 
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
            String errMsg = "FileHashStore.writeToTmpMetadataFile - Unexpected IOException encountered: "
                    + ioe.getMessage();
            logFileHashStore.error(errMsg);
            throw ioe;

        } finally {
            os.flush();
            os.close();
        }
    }

    /**
     * Determines whether a given directory is empty or not
     * 
     * @param directory Directory to check
     * @return False if not empty
     * @throws IOException If I/O occurs when calling Files for a new directory
     *                     stream
     */
    private static boolean isDirectoryEmpty(Path directory) throws IOException {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            return !stream.iterator().hasNext();
        }
    }
}
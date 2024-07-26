package org.dataone.hashstore.hashstoreconverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FileHashStoreLinks extends FileHashStore {

    private static final Log logFileHashStoreLinks = LogFactory.getLog(FileHashStore.class);
    private final int DIRECTORY_DEPTH;
    private final int DIRECTORY_WIDTH;
    private final String OBJECT_STORE_ALGORITHM;
    private final Path OBJECT_STORE_DIRECTORY;

    public FileHashStoreLinks(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        super(hashstoreProperties);
        // If configuration matches, set FileHashStoreLinks private variables
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
        DIRECTORY_DEPTH = storeDepth;
        DIRECTORY_WIDTH = storeWidth;
        OBJECT_STORE_ALGORITHM = storeAlgorithm;
        OBJECT_STORE_DIRECTORY = storePath.resolve("objects");
        logFileHashStoreLinks.info("FileHashStoreLinks initialized");
    }

    /**
     * Creates a hard link to a data file and return a ObjectMetadata of the given data object.
     *
     * @param filePath   Path to the source file which a hard link will be created for
     * @param fileStream Stream to the source file to calculate checksums for
     * @param pid        Persistent or authority-based identifier for tagging
     * @return ObjectMetadata encapsulating information about the data file
     * @throws NoSuchAlgorithmException Issue with one of the hashing algorithms to calculate
     * @throws IOException              An issue with reading from the given file stream
     * @throws InterruptedException Sync issue when tagging pid and cid
     */
    public ObjectMetadata storeHardLink(Path filePath, InputStream fileStream, String pid)
        throws NoSuchAlgorithmException, IOException, InterruptedException {
        // Validate input parameters
        FileHashStoreUtility.ensureNotNull(filePath, "filePath", "storeHardLink");
        FileHashStoreUtility.ensureNotNull(fileStream, "fileStream", "storeHardLink");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "storeHardLink");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "storeHardLink");
        if (!Files.exists(filePath)) {
            String errMsg = "Given file path: " + filePath + " does not exist.";
            throw new FileNotFoundException(errMsg);
        }

        try {
            Map<String, String> hexDigests = generateChecksums(fileStream, null, null);
            // Gather the elements to form the permanent address
            String objectCid = hexDigests.get(OBJECT_STORE_ALGORITHM);
            String objRelativePath = FileHashStoreUtility.getHierarchicalPathString(
                DIRECTORY_DEPTH, DIRECTORY_WIDTH, objectCid
            );
            Path objHardLinkPath = OBJECT_STORE_DIRECTORY.resolve(objRelativePath);
            // Create parent directories to the hard link, otherwise
            // Files.createLink will throw a NoSuchFileException
            File destinationDirectory = new File(objHardLinkPath.toFile().getParent());
            if (!destinationDirectory.exists()) {
                Path destinationDirectoryPath = destinationDirectory.toPath();
                try {
                    Files.createDirectories(destinationDirectoryPath);

                } catch (FileAlreadyExistsException faee) {
                    logFileHashStoreLinks.warn(
                        "Directory already exists at: " + destinationDirectoryPath
                            + " - Skipping directory creation");
                }
            }
            // Finish the contract
            Files.createLink(objHardLinkPath, filePath);
            // This method is thread safe and synchronized
            tagObject(pid, objectCid);
            logFileHashStoreLinks.info(
                "Hard link has been created for pid:" + pid + " with cid: " + objectCid
                    + " has been tagged");

            return new ObjectMetadata(pid, objectCid, Files.size(objHardLinkPath), hexDigests);

        } finally {
            // Close stream
            fileStream.close();
        }
    }

    /**
     * Get a HashStore data object path
     *
     * @param pid Persistent or authority-based identifier
     * @return Path to a HashStore data object
     * @throws NoSuchAlgorithmException Conflicting algorithm preventing calculation of the path
     * @throws IOException              If there is an issue with reading from the pid refs file
     */
    protected Path getHashStoreLinksDataObjectPath(String pid)
        throws NoSuchAlgorithmException, IOException {
        return getHashStoreDataObjectPath(pid);
    }

    /**
     * Get a HashMap consisting of algorithms and their respective hex digests for a given
     * data stream. If an additional algorithm is supplied and supported, it and its checksum
     * value will be included in the hex digests map. Default algorithms: MD5, SHA-1, SHA-256,
     * SHA-384, SHA-512
     *
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
    protected Map<String, String> generateChecksums(InputStream dataStream,
                                                  String additionalAlgorithm, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, SecurityException {
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

        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.getName());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.getName());
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.getName());
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.getName());
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.getName());
        MessageDigest additionalAlgo = null;
        MessageDigest checksumAlgo = null;
        if (generateAddAlgo) {
            logFileHashStoreLinks.debug(
                "Adding additional algorithm to hex digest map, algorithm: " + additionalAlgorithm);
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (generateCsAlgo) {
            logFileHashStoreLinks.debug(
                "Adding checksum algorithm to hex digest map, algorithm: " + checksumAlgorithm);
            checksumAlgo = MessageDigest.getInstance(checksumAlgorithm);
        }

        // Calculate hex digests
        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
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
            logFileHashStoreLinks.error(errMsg);
            throw ioe;

        } finally {
            dataStream.close();
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

        return hexDigests;
    }

}

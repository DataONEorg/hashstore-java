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

import javax.xml.bind.DatatypeConverter;

/**
 * HashFileStore handles IO operations for HashStore
 */
public class HashFileStore {
    private final int directoryDepth;
    private final int directoryWidth;
    private final String objectStoreAlgorithm;
    private final Path objectStoreDirectory;
    private final Path tmpFileDirectory;
    public String[] supportedHashAlgorithms = { "MD2", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512", "SHA-512/224",
            "SHA-512/256" };

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
        boolean algorithmSupported = this.isValidAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new IllegalArgumentException(
                    "Algorithm not supported. Supported algorithms: " +
                            Arrays.toString(this.supportedHashAlgorithms));
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
    public HashAddress putObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
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
            boolean algorithmSupported = this.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Additional algorithm not supported - unable to generate additional hex digest value. additionalAlgorithm: "
                                + additionalAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(this.supportedHashAlgorithms));
            }
        }
        if (checksumAlgorithm != null) {
            boolean checksumAlgorithmSupported = this.isValidAlgorithm(checksumAlgorithm);
            if (!checksumAlgorithmSupported) {
                // TODO: Log failure - include signature values
                throw new IllegalArgumentException(
                        "Checksum algorithm not supported - cannot be used to validate object. checksumAlgorithm: "
                                + checksumAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(this.supportedHashAlgorithms));
            }
        }

        // Gather HashAddress elements and prepare object permanent address
        String objAuthorityId = this.getHexDigest(pid, this.objectStoreAlgorithm);
        String objShardString = this.shard(this.directoryDepth, this.directoryWidth, objAuthorityId);
        String objAbsolutePathString = this.objectStoreDirectory.toString() + "/" + objShardString;
        File objHashAddress = new File(objAbsolutePathString);
        // If file (pid hash) exists, reject request immediately
        if (objHashAddress.exists()) {
            throw new FileAlreadyExistsException("File already exists for pid: " + pid);
        }

        // Generate tmp file and write to it
        File tmpFile = this.generateTmpFile("tmp", this.tmpFileDirectory.toFile());
        Map<String, String> hexDigests = this.writeToTmpFileAndGenerateChecksums(tmpFile, object,
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
        boolean isNotDuplicate = this.move(tmpFile, objHashAddress);
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

    /**
     * Checks whether a given algorithm is supported based on the HashUtil class
     * variable supportedHashAlgorithms
     * 
     * @param algorithm
     * @return boolean that describes whether an algorithm is supported
     * @throws NullPointerException
     */
    protected boolean isValidAlgorithm(String algorithm) throws NullPointerException {
        if (algorithm == null) {
            throw new NullPointerException("algorithm supplied is null: " + algorithm);
        }
        if (!Arrays.asList(this.supportedHashAlgorithms).contains(algorithm) && algorithm != null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Given a string and supported algorithm returns the hex digest
     * 
     * @param string    authority based identifier or persistent identifier
     * @param algorithm
     * 
     * @return Hex digest of the given string in lower-case
     * @throws IllegalArgumentException
     * @throws NoSuchAlgorithmException
     */
    protected String getHexDigest(String string, String algorithm) throws NoSuchAlgorithmException {
        if (algorithm == null || algorithm.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "Algorithm cannot be null or empty");
        }
        if (string == null || string.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "String cannot be null or empty");
        }
        boolean algorithmSupported = this.isValidAlgorithm(algorithm);
        if (!algorithmSupported) {
            throw new NoSuchAlgorithmException(
                    "Algorithm not supported. Supported algorithms: " + Arrays.toString(supportedHashAlgorithms));
        }
        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        String stringDigest = DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
        return stringDigest;
    }

    /**
     * Generates a hierarchical path by dividing a given digest into tokens
     * of fixed width, and concatenating them with '/' as the delimiter.
     *
     * @param depth
     * @param width
     * @param digest
     * @return String
     */
    protected String shard(int depth, int width, String digest) {
        List<String> tokens = new ArrayList<String>();
        int digestLength = digest.length();
        for (int i = 0; i < depth; i++) {
            int start = i * width;
            int end = Math.min((i + 1) * width, digestLength);
            tokens.add(digest.substring(start, end));
        }
        if (depth * width < digestLength) {
            tokens.add(digest.substring(depth * width));
        }
        List<String> stringArray = new ArrayList<String>();
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
     * @param prefix
     * @param directory
     * 
     * @return Temporary file (File) ready to write into
     * @throws IOException
     * @throws SecurityException
     */
    protected File generateTmpFile(String prefix, File directory) throws IOException, SecurityException {
        String newPrefix = prefix + "-" + System.currentTimeMillis();
        String suffix = null;
        File newFile = null;
        try {
            newFile = File.createTempFile(newPrefix, suffix, directory);
        } catch (IOException ioe) {
            // TODO: Log Exception ioe
            throw new IOException("Unable to generate tmpFile. IOException: " + ioe.getMessage());
        } catch (SecurityException se) {
            // TODO: Log Exception se
            throw new SecurityException("File not allowed (security manager exists): " + se.getMessage());
        }
        // TODO: Log - newFile.getCanonicalPath());
        return newFile;
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap
     * consisting of algorithms and their respective hex digests. If an additional
     * algorithm is supplied and supported, it and its checksum value will be
     * included in the hex digests map.
     * 
     * Default algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
     * 
     * @param tmpFile
     * @param dataStream
     * @param additionalAlgorithm
     * 
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws SecurityException
     * @throws FileNotFoundException
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
                        "Algorithm not supported. Supported algorithms: " + Arrays.toString(supportedHashAlgorithms));
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
        } finally {
            try {
                os.flush();
                os.close();
            } catch (Exception e) {
                // TODO: Log exception
            }
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
            String extraDigest = DatatypeConverter.printHexBinary(extraAlgo.digest()).toLowerCase();
            hexDigests.put(additionalAlgorithm, extraDigest);
        }

        return hexDigests;
    }

    /**
     * Moves an object from one location to another if the object does not exist
     * 
     * @param source
     * @param target
     * 
     * @return boolean to confirm file is not a duplicate and has been moved
     * @throws IOException
     */
    protected boolean move(File source, File target) throws IOException, SecurityException {
        boolean wasMoved = false;
        if (target.exists()) {
            return wasMoved;
        } else {
            File destinationDirectory = new File(target.getParent());
            // Create parent directory if it doesn't exist
            if (!destinationDirectory.exists()) {
                Path destinationDirectoryPath = destinationDirectory.toPath();
                Files.createDirectories(destinationDirectoryPath);
            }

            // Move file
            Path sourceFilePath = source.toPath();
            Path targetFilePath = target.toPath();
            wasMoved = true;
            try {
                Files.move(sourceFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException amnse) {
                // TODO: Log exception and specify atomic_move not possible
                Files.move(sourceFilePath, targetFilePath);
            } catch (IOException ioe) {
                // TODO: Log failure - include signature values, ioe
                throw ioe;
            }
        }
        return wasMoved;
    }
}
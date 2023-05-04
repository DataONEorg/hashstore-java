package org.dataone.hashstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.hashfs.HashAddress;
import org.dataone.hashstore.hashfs.HashFileStore;
import org.dataone.hashstore.interfaces.HashStoreInterface;

/**
 * HashStore is a content-addressable file management system that utilizes a
 * persistent identifier (PID) in the form of a hex digest value to address
 * files. The system stores files in a file store and provides an API for
 * interacting with the store. The API should implement the HashStoreInterface
 * to ensure proper usage of the system.
 */
public class HashStore implements HashStoreInterface {
    private static final Log logHashStore = LogFactory.getLog(HashStore.class);
    private final HashFileStore hashfs;
    private final int depth = 3;
    private final int width = 2;
    private final String algorithm = "SHA-256";
    private final static int TIME_OUT_MILLISEC = 1000;
    private final static ArrayList<String> objectLockedIds = new ArrayList<>(100);

    /**
     * Default constructor for HashStore
     * 
     * @param storeDirectory Full file path (ex. /usr/org/metacat/objects)
     * @throws IllegalArgumentException Depth, width must be greater than 0
     * @throws IOException              Issue when creating storeDirectory
     */
    public HashStore(Path storeDirectory)
            throws IllegalArgumentException, IOException {
        try {
            this.hashfs = new HashFileStore(this.depth, this.width, this.algorithm, storeDirectory);
        } catch (IllegalArgumentException iae) {
            logHashStore.error("Unable to initialize HashFileStore - Illegal Argument Exception: " + iae.getMessage());
            throw iae;
        }
    }

    /**
     * Store an object to the storeDirectory.
     * 
     * The permanent address is the SHA-256 hex digest of a given string that
     * represents an authority based identifier (ex. pid)
     * 
     * Returns a HashAddress object that contains the file id, relative path,
     * absolute path, duplicate status and a checksum map based on a default
     * algorithm list.
     * 
     * @param object              Input stream to file
     * @param pid                 Authority-based idenetifier
     * @param additionalAlgorithm Additional hex digest to include in hexDigests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
     * 
     * @return A HashAddress object that contains the file id, relative path,
     *         absolute path, duplicate status and a checksum map based on the
     *         default algorithm list.
     * @throws NoSuchAlgorithmException   When additiionalAlgorithm or
     *                                    checksumAlgorithm is invalid
     * @throws IOException                I/O Error when writing file, generating
     *                                    checksums and moving file
     * @throws SecurityException          Insufficient permissions to read/access
     *                                    files or when generating/writing to a file
     * @throws FileNotFoundException      When file tmpFile not found during store
     * @throws FileAlreadyExistsException Duplicate object in store exists
     * @throws IllegalArgumentException   When signature values are unexpectedly
     *                                    empty (checksum, pid, etc.)
     * @throws NullPointerException       Arguments are null for pid or object
     * @throws InterruptedException       Synchronization issue with objectLockedIds
     */
    @Override
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException, InterruptedException {
        logHashStore.info("HashStore.storeObject - Called to store object for pid: " + pid);
        // Begin input validation
        if (object == null) {
            logHashStore.error("HashStore.storeObject - InputStream cannot be null, pid: " + pid);
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        if (pid == null || pid.trim().isEmpty()) {
            logHashStore.error("HashStore.storeObject - pid cannot be null or empty, pid: " + pid);
            throw new IllegalArgumentException("Pid cannot be null or empty, pid: " + pid);
        }
        // Checksum cannot be empty or null if checksumAlgorithm is passed
        if (checksumAlgorithm != null & checksum != null) {
            if (checksum.trim().isEmpty()) {
                logHashStore
                        .error("HashStore.storeObject - checksum cannot be empty if checksumAlgorithm is supplied, pid: "
                                + pid);
                throw new IllegalArgumentException(
                        "Checksum cannot be empty when checksumAlgorithm is supplied.");
            }
        }
        // Cannot generate additional or checksum algorithm if it is not supported
        if (additionalAlgorithm != null) {
            boolean algorithmSupported = this.hashfs.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                logHashStore.error("HashStore.storeObject - additionalAlgorithm is not supported."
                        + "additionalAlgorithm: " + additionalAlgorithm + ". pid: " + pid);
                throw new IllegalArgumentException(
                        "Additional algorithm not supported - unable to generate additional hex digest value. additionalAlgorithm: "
                                + additionalAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(HashFileStore.SUPPORTED_HASH_ALGORITHMS));
            }
        }
        // Check support for checksumAlgorithm
        if (checksumAlgorithm != null) {
            boolean checksumAlgorithmSupported = this.hashfs.isValidAlgorithm(checksumAlgorithm);
            if (!checksumAlgorithmSupported) {
                logHashStore
                        .error("HashStore.storeObject - checksumAlgorithm not supported, checksumAlgorithm: "
                                + checksumAlgorithm + ". pid: " + pid);
                throw new IllegalArgumentException(
                        "Checksum algorithm not supported - cannot be used to validate object. checksumAlgorithm: "
                                + checksumAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(HashFileStore.SUPPORTED_HASH_ALGORITHMS));
            }
        }

        // Lock pid for thread safety, transaction control and atomic writing
        // A pid can only be stored once and only once, subsequent calls will
        // be accepted but will be rejected if pid hash object exists
        synchronized (objectLockedIds) {
            while (objectLockedIds.contains(pid)) {
                try {
                    logHashStore.warn("HashStore.storeObject - Duplicate object request encountered for pid: " + pid
                            + ". Lock is waiting for pid to be released.");
                    objectLockedIds.wait(TIME_OUT_MILLISEC);
                } catch (InterruptedException ie) {
                    logHashStore.error(
                            "HashStore.storeObject - objectLockedIds synchronization has been interrupted for pid: "
                                    + pid + ". Exception: " + ie.getMessage());
                    throw ie;
                }
            }
            objectLockedIds.add(pid);
        }

        try {
            logHashStore.debug("HashStore.storeObject - hashfs.putObject request for pid: " + pid
                    + ". additionalAlgorithm: " + additionalAlgorithm + ". checksum: " + checksum
                    + ". checksumAlgorithm: " + checksumAlgorithm);
            // Store object
            HashAddress objInfo = this.hashfs.putObject(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
            logHashStore.info(
                    "HashStore.storeObject - Object stored for pid: " + pid + ". Permanent address: "
                            + objInfo.getAbsPath());
            return objInfo;
        } catch (NullPointerException npe) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". NullPointerException: " + npe.getMessage());
            throw npe;
        } catch (IllegalArgumentException iae) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". IllegalArgumentException: " + iae.getMessage());
            throw iae;
        } catch (NoSuchAlgorithmException nsae) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". NoSuchAlgorithmException: " + nsae.getMessage());
            throw nsae;
        } catch (FileAlreadyExistsException faee) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". FileAlreadyExistsException: " + faee.getMessage());
            throw faee;
        } catch (FileNotFoundException fnfe) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". FileNotFoundException: " + fnfe.getMessage());
            throw fnfe;
        } catch (IOException ioe) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". IOException: " + ioe.getMessage());
            throw ioe;
        } catch (SecurityException se) {
            logHashStore.error("HashStore.storeObject - Cannot store object for pid: " + pid
                    + ". SecurityException: " + se.getMessage());
            throw se;
        } catch (RuntimeException re) {
            logHashStore.error("HashStore.storeObject - Object was stored for : " + pid
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
}

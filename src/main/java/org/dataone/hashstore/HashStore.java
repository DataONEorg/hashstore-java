package org.dataone.hashstore;

import java.io.BufferedReader;
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
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.filehashstore.HashAddress;
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
    private final FileHashStore filehs;
    private final int depth = 3;
    private final int width = 2;
    private final String algorithm = "SHA-256";
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
            this.filehs = new FileHashStore(this.depth, this.width, this.algorithm, storeDirectory);
        } catch (IllegalArgumentException iae) {
            logHashStore
                    .error("Unable to initialize FileHashStore - storeDirectory supplied: " + storeDirectory.toString()
                            + ". Illegal Argument Exception: " + iae.getMessage());
            throw iae;
        }
    }

    @Override
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException, RuntimeException {
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
        // Cannot generate additional or checksum algorithm if it is not supported
        if (additionalAlgorithm != null) {
            boolean algorithmSupported = this.filehs.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                logHashStore.error("HashStore.storeObject - additionalAlgorithm is not supported."
                        + "additionalAlgorithm: " + additionalAlgorithm + ". pid: " + pid);
                throw new IllegalArgumentException(
                        "Additional algorithm not supported - unable to generate additional hex digest value. additionalAlgorithm: "
                                + additionalAlgorithm + ". Supported algorithms: "
                                + Arrays.toString(FileHashStore.SUPPORTED_HASH_ALGORITHMS));
            }
        }
        // checksumAlgorithm and checksum must both be present if validation is desired
        this.filehs.validateChecksumParameters(checksum, checksumAlgorithm, additionalAlgorithm);

        // Lock pid for thread safety, transaction control and atomic writing
        // A pid can only be stored once and only once, subsequent calls will
        // be accepted but will be rejected if pid hash object exists
        synchronized (objectLockedIds) {
            if (objectLockedIds.contains(pid)) {
                logHashStore.warn("HashStore.storeObject - Duplicate object request encountered for pid: " + pid);
                throw new RuntimeException("HashStore.storeObject request for pid: " + pid + " already in progress.");
            }
            objectLockedIds.add(pid);
        }

        try {
            logHashStore.debug("HashStore.storeObject - filehs.putObject request for pid: " + pid
                    + ". additionalAlgorithm: " + additionalAlgorithm + ". checksum: " + checksum
                    + ". checksumAlgorithm: " + checksumAlgorithm);
            // Store object
            HashAddress objInfo = this.filehs.putObject(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
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
}

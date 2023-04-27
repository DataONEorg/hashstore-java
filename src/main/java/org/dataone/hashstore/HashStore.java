package org.dataone.hashstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;

import org.dataone.hashstore.hashfs.HashAddress;
import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * HashStore is a file store system/content-addressable file manager that uses
 * the object's hex digest value as the file address
 */
public class HashStore {
    private int depth = 3;
    private int width = 2;
    private String sysmetaNameSpace = "http://ns.dataone.org/service/types/v2.0";
    private String algorithm = "SHA-256";
    private HashFileStore hashfs;
    private final static int TIME_OUT_MILLISEC = 1000;
    // Shared class variable amongst all instances
    private static ArrayList<String> objectLockedIds = new ArrayList<String>(100);

    /**
     * Default constructor for HashStore
     * 
     * @param storeDirectory Full file path (ex. /usr/org/metacat/objects)
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public HashStore(Path storeDirectory)
            throws IllegalArgumentException, IOException {
        try {
            hashfs = new HashFileStore(this.depth, this.width, this.algorithm, storeDirectory);
        } catch (IllegalArgumentException e) {
            // TODO: Log failure - include signature values, e
            throw e;
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
     * @return
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
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException, InterruptedException {
        if (object == null) {
            throw new NullPointerException("Invalid input stream, data is null.");
        }
        if (pid == null || pid.isEmpty()) {
            throw new IllegalArgumentException("Pid cannot be null or empty, pid: " + pid);
        }

        // Lock pid for thread safety, transaction control and atomic writing
        // A pid can only be stored once and only once, subsequent calls will
        // be accepted but will be rejected if pid hash object exists
        synchronized (objectLockedIds) {
            while (objectLockedIds.contains(pid)) {
                try {
                    objectLockedIds.wait(TIME_OUT_MILLISEC);
                } catch (InterruptedException ie) {
                    // TODO: Log failure - include signature values, ie
                    throw ie;
                }
            }
            objectLockedIds.add(pid);
        }
        // Store object
        try {
            HashAddress objInfo = this.hashfs.putObject(object, pid, additionalAlgorithm, checksum, checksumAlgorithm);
            return objInfo;
        } catch (NullPointerException npe) {
            // TODO: Log failure - include signature values, npe
            throw npe;
        } catch (IllegalArgumentException iae) {
            // TODO: Log failure - include signature values, iae
            throw iae;
        } catch (NoSuchAlgorithmException nsae) {
            // TODO: Log failure - include signature values, nsae
            throw nsae;
        } catch (FileAlreadyExistsException faee) {
            // TODO: Log failure - include signature values, faee
            throw faee;
        } catch (FileNotFoundException fnfe) {
            // TODO: Log failure - include signature values, fnfe
            throw fnfe;
        } catch (IOException ioe) {
            // TODO: Log failure - include signature values, ioe
            throw ioe;
        } catch (SecurityException se) {
            // TODO: Log failure - include signature values, se
            throw se;
        } finally {
            // Release lock
            try {
                synchronized (objectLockedIds) {
                    objectLockedIds.remove(pid);
                    objectLockedIds.notifyAll();
                }
            } catch (RuntimeException re) {
                // TODO: Log failure - include signature values, re
                throw re;
            }
        }
    }
}

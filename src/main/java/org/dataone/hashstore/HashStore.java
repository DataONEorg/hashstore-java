package org.dataone.hashstore;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

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

    /**
     * Default constructor for HashStore
     * 
     * @param storeDirectory Full file path (ex. /usr/org/metacat/objects)
     * @throws IllegalArgumentException
     * @throws IOException
     */
    public HashStore(String storeDirectory)
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
     * The permanent address is the SHA256 hex digest of a given string that
     * represents an authority based identifer (pid).
     * 
     * Returns a HashAddress object that contains the file id, relative path,
     * absolute path, duplicate status and a checksum map based on a default
     * algorithm list.
     * 
     * @param pid
     * @param data
     * @param additionalAlgorithm
     * @param checksum
     * @return
     * @throws NoSuchAlgorithmException
     * @throws IOException
     */
    public HashAddress storeObject(String pid, InputStream data, String additionalAlgorithm, String checksum)
            throws NoSuchAlgorithmException, IOException {
        try {
            HashAddress objInfo = this.hashfs.putObject(data, pid, additionalAlgorithm, checksum);
            return objInfo;
        } catch (NoSuchAlgorithmException e) {
            // TODO: Log failure - include signature values, e
            throw e;
        } catch (IOException e) {
            // TODO: Log failure - include signature values, e
            throw e;
        }
    }
}

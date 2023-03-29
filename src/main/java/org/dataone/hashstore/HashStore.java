package org.dataone.hashstore;

import java.io.IOException;

import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * HashStore is a file store system/content-addressable file manager that uses
 * the object's hex digest value as the file address
 */
public class HashStore {
    private int depth = 3;
    private int width = 2;
    private String sysmetaNameSpace = "http://ns.dataone.org/service/types/v2.0";
    private String algorithm = "sha256";
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
}

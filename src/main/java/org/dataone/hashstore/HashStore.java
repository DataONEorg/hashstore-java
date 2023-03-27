package org.dataone.hashstore;

import java.io.IOException;

import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * HashStore is a file store system that uses the object's hex digest value as
 * the file address and stores the respective sysmeta documents in a separate
 * directory using the given identifier's (pid) hex digest value.
 */
public class HashStore {
    private String sysmetaNameSpace = "http://ns.dataone.org/service/types/v2.0";
    private String algorithm = "sha256";
    private int depth = 3;
    private int width = 2;

    public HashStore(String storeDirectory)
            throws IllegalArgumentException, IOException {
        try {
            HashFileStore hashfs = new HashFileStore(depth, width, algorithm, storeDirectory);
        } catch (IllegalArgumentException e) {
            // TODO: Log failure - include signature values, e
            throw e;
        }

    }
}

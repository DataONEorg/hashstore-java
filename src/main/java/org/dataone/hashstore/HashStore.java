package org.dataone.hashstore;

import org.dataone.hashstore.hashfs.HashFileStore;

/**
 * HashStore is a file store system that uses the object's hex digest value as
 * the file address and stores the respective sysmeta documents in a separate
 * directory using the given identifier's (pid) hex digest value.
 */
public class HashStore {
    private String sysmetaNameSpace;

    public HashStore(int depth, int width, String algorithm, String storeDirectory, String namespace) {
        this.sysmetaNameSpace = namespace;
        try {
            HashFileStore hashfs = new HashFileStore(depth, width, algorithm, storeDirectory);
        } catch (IllegalArgumentException e) {
            // TODO: Log failure - include signature values, e
            return;
        }

    }
}

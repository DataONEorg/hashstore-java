package org.dataone.hashstore;

/**
 * HashStore is a file store system that uses the object's hex digest value as
 * the file address and stores the respective sysmeta documents in a separate
 * directory using the given identifier's (pid) hex digest value.
 */
public class HashStore {
    private String sysmetaNameSpace;

    public HashStore(byte depth, byte width, String namespace) {
        this.sysmetaNameSpace = namespace;
        // TODO: Initialize File Store
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");
    }
}

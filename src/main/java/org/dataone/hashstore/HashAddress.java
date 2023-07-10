package org.dataone.hashstore;

import java.util.Map;

/**
 * HashAddress is a class that models a unique identifier for a file in the
 * Hashstore. It encapsulates information about the file's name, path, and
 * associated hash digest values. By using HashAddress objects, client code can
 * easily locate, retrieve, and modify files in the HashStore without needing to
 * know the underlying file system details.
 */
public class HashAddress {
    private final String id;
    private final boolean isDuplicate;
    private final Map<String, String> hexDigests;

    /**
     * Creates a new instance of HashAddress with the given properties.
     *
     * @param id          Unique identifier for the file
     * @param relPath     Relative path of the file within the hash store
     * @param absPath     Absolute path of the file on the local file system
     * @param isDuplicate Flag indicating if the file is a duplicate of an
     *                    existing file
     * @param hexDigests  A map of hash algorithm names to their hex-encoded
     *                    digest values for the file
     */
    public HashAddress(String id, boolean isDuplicate,
            Map<String, String> hexDigests) {
        this.id = id;
        this.isDuplicate = isDuplicate;
        this.hexDigests = hexDigests;
    }

    /**
     * Return the id (address) of the file
     * 
     * @return id
     */
    public String getId() {
        return id;
    }

    /**
     * Return the flag of whether a file is a duplicate or not
     * 
     * @return True if the file is not a duplicate, false otherwise
     */
    public boolean getIsDuplicate() {
        return isDuplicate;
    }

    /**
     * Return a map of hex digests
     * 
     * @return hexDigests
     */
    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}
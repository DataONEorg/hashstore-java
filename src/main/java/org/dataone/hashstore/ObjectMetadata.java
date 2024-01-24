package org.dataone.hashstore;

import java.util.Map;

/**
 * ObjectMetadata is a class that models a unique identifier for an object in the HashStore. It
 * encapsulates information about a file's content identifier (cid), size, and associated hash
 * digest values. By using ObjectMetadata objects, client code can easily obtain metadata of a store
 * object in HashStore without needing to know the underlying file system details.
 */
public class ObjectMetadata {
    private final String cid;
    private final long size;
    private final Map<String, String> hexDigests;

    /**
     * Creates a new instance of ObjectMetadata with the given properties.
     *
     * @param cid        Unique identifier for the file
     * @param size       Size of stored file
     * @param hexDigests A map of hash algorithm names to their hex-encoded digest values for the
     *                   file
     */
    public ObjectMetadata(String cid, long size, Map<String, String> hexDigests) {
        this.cid = cid;
        this.size = size;
        this.hexDigests = hexDigests;
    }

    /**
     * Return the cid (content identifier) of the file
     * 
     * @return cid
     */
    public String getCid() {
        return cid;
    }

    /**
     * Return the size of the file
     * 
     * @return size
     */
    public long getSize() {
        return size;
    }

    /**
     * Return a map of hex digests (checksums)
     * 
     * @return hexDigests
     */
    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}

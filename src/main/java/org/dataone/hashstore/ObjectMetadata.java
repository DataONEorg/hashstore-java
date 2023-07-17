package org.dataone.hashstore;

import java.util.Map;

/**
 * ObjectMetadata is a class that models a unique identifier for an object in the Hashstore. It
 * encapsulates information about a file's name, path, and associated hash digest values. By using
 * ObjectMetadata objects, client code can easily obtain metadata of a store object in HashStore
 * without needing to know the underlying file system details.
 */
public class ObjectMetadata {
    private final String id;
    private final long size;
    private final boolean isDuplicate;
    private final Map<String, String> hexDigests;

    /**
     * Creates a new instance of ObjectMetadata with the given properties.
     *
     * @param id          Unique identifier for the file
     * @param isDuplicate Flag indicating if the file is a duplicate of an existing file
     * @param hexDigests  A map of hash algorithm names to their hex-encoded digest values for the
     *                    file
     */
    public ObjectMetadata(
        String id, long size, boolean isDuplicate, Map<String, String> hexDigests
    ) {
        this.id = id;
        this.isDuplicate = isDuplicate;
        this.hexDigests = hexDigests;
        this.size = size;
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
     * Return the size of the file
     * 
     * @return id
     */
    public long getSize() {
        return size;
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

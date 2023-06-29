package org.dataone.hashstore;

import java.nio.file.Path;
import java.util.Map;

/**
 * HashAddress is a class that models a unique identifier for a file in the
 * Hashstore. It encapsulates information about the file's name, path, and
 * associated hash digest values. By using HashAddress objects, client code can
 * easily locate, retrieve, and modify files in the Hashstore without needing to
 * know the underlying file system details.
 */
public class HashAddress {
    private final String id;
    private final String relPath;
    private final Path absPath;
    private final boolean isDuplicate;
    private final Map<String, String> hexDigests;

    /**
     * Creates a new instance of HashAddress with the given properties.
     *
     * @param id          the unique identifier for the file
     * @param relPath     the relative path of the file within the hash store
     * @param absPath     the absolute path of the file on the local file system
     * @param isDuplicate a flag indicating if the file is a duplicate of an
     *                    existing file
     * @param hexDigests  a map of hash algorithm names to their hex-encoded
     *                    digest values for the file
     */
    public HashAddress(String id, String relPath, Path absPath, boolean isDuplicate,
            Map<String, String> hexDigests) {
        // Constructor implementation
        this.id = id;
        this.relPath = relPath;
        this.absPath = absPath;
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
     * Return the relative path to the file
     * 
     * @return relative path
     */
    public String getRelPath() {
        return relPath;
    }

    /**
     * Return the absolute path to the file
     * 
     * @return absolute path
     */
    public Path getAbsPath() {
        return absPath;
    }

    /**
     * Return the flag of whether a file is a duplicate or not
     * 
     * @return true if the file is not a duplicate
     */
    public boolean getIsDuplicate() {
        return isDuplicate;
    }

    /**
     * Return a map of hex digests
     * 
     * @return hex digest map
     */
    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}
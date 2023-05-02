package org.dataone.hashstore.interfaces;

import java.io.InputStream;
import org.dataone.hashstore.hashfs.HashAddress;

public interface HashStoreInterface {
    /**
     * storeObject must atomically store an object to disk given an InputStream
     * and a pid (authority-based identifier) and return an HashAddress object
     * containing the file's id, relative path, absolute path, duplicate object
     * status and hex digest map of algorithms and checksums. Note, the file's id
     * is the SHA-256 hex digest of a given persistent identifier (pid).
     * 
     * Additionally, if supplied with an additionalAlgorithm, it must add the
     * algorithm and its respective hex digest to the hex digest map. If supplied
     * with a checksum and a checksumAlgorithm, it must validate that the object
     * matches what is provided before moving the file to its permanent address.
     * 
     * @param object              Input stream to file
     * @param pid                 Authority-based idenetifier
     * @param additionalAlgorithm Additional hex digest to include in hexDigests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
     * @return
     * @throws Exception Various exceptions depending on the implementation
     */
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm) throws Exception;
}

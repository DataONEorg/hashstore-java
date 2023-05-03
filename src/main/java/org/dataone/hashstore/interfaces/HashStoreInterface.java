package org.dataone.hashstore.interfaces;

import java.io.InputStream;
import org.dataone.hashstore.hashfs.HashAddress;

public interface HashStoreInterface {
    /**
     * The storeObject method is responsible for the atomic storage of objects to
     * disk using a given InputStream and a persistent identifier (pid). Upon
     * successful storage, the method returns a HashAddress object containing
     * relevant file information, such as the file's id, relative path, absolute
     * path, duplicate object status, and hex digest map of algorithms and
     * checksums.
     * 
     * The file's id is determined using the SHA-256 hex digest of the provided pid,
     * which is also used as the permanent address of the file. The method ensures
     * that an object is stored only once by synchronizing multiple calls and
     * rejecting the ones with already-existing objects. If an additionalAlgorithm
     * is provided, storeObject adds the algorithm and its corresponding hex digest
     * to the hex digest map if it is supported. Similarly, if a checksum and a
     * supported checksumAlgorithm are provided, the method validates the object to
     * ensure it matches what is provided before moving the file to its permanent
     * address.
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

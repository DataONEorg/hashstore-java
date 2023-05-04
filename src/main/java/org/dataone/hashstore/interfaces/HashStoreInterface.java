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
     * checksums. `storeObject` also ensures that an object is stored only once by
     * synchronizing multiple calls and rejecting calls to store duplicate objects.
     * 
     * The file's id is determined by calculating the SHA-256 hex digest of the
     * provided pid, which is also used as the permanent address of the file. The
     * file's identifier is then sharded using a depth of 3 and width of 2,
     * delimited by '/' and concatenated to produce the final permanent address
     * and is stored in the `/[...storeDirectory]/objects/` directory.
     * 
     * By default, the hex digest map includes the following hash algorithms: MD5,
     * SHA-1, SHA-256, SHA-384 and SHA-512, which are the most commonly used
     * algorithms in dataset submissions to DataONE and the Arctic Data Center. If
     * an additional algorithm is provided, the `storeObject` method checks if it is
     * supported and adds it to the map along with its corresponding hex digest. An
     * algorithm is considered "supported" if it is recognized as a valid hash
     * algorithm in the `java.security.MessageDigest` class.
     * 
     * Similarly, if a checksum and a checksumAlgorithm value are provided,
     * `storeObject` validates the object to ensure it matches what is provided
     * before moving the file to its permanent address.
     * 
     * @param object              Input stream to file
     * @param pid                 Authority-based identifier
     * @param additionalAlgorithm Additional hex digest to include in hexDigests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
     * @return HashAddress object encapsulating file information
     * @throws Exception Various exceptions depending on the implementation
     */
    HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm) throws Exception;
}

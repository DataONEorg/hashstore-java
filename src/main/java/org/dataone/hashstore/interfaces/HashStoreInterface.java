package org.dataone.hashstore.interfaces;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.security.NoSuchAlgorithmException;

import org.dataone.hashstore.HashAddress;

public interface HashStoreInterface {
    /**
     * The `storeObject` method is responsible for the atomic storage of objects to
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
     * @throws NoSuchAlgorithmException        When additionalAlgorithm or
     *                                         checksumAlgorithm is invalid
     * @throws IOException                     I/O Error when writing file,
     *                                         generating checksums and moving file
     * @throws SecurityException               Insufficient permissions to
     *                                         read/access files or when
     *                                         generating/writing to a file
     * @throws FileNotFoundException           tmpFile not found when writing
     *                                         from stream
     * @throws FileAlreadyExistsException      Duplicate object in store exists
     *                                         during move call
     * @throws IllegalArgumentException        Signature values are unexpectedly
     *                                         empty (checksum, pid, etc.)
     * @throws NullPointerException            Arguments are null for pid or object
     * @throws RuntimeException                Attempting to store pid object
     *                                         that is already in progress
     * @throws AtomicMoveNotSupportedException Attempting to move files across
     *                                         file systems
     */
    HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm)
            throws NoSuchAlgorithmException, IOException, SecurityException, FileNotFoundException,
            FileAlreadyExistsException, IllegalArgumentException, NullPointerException, RuntimeException;

    /**
     * The `storeSysmeta` method is responsible for adding and/or updating metadata
     * (`sysmeta`) to disk using a given InputStream and a persistent identifier
     * (pid). The metadata object consists of a header and body portion. The header
     * is formed by writing the namespace/format (utf-8) of the metadata document
     * followed by a null character `\x00` and the body follows immediately after.
     * 
     * Upon successful storage of sysmeta, the method returns a String that
     * represents the file's permanent address, and similarly to storeObject, this
     * permanent address is determined by calculating the SHA-256 hex digest of the
     * provided pid. Finally, sysmeta are stored in parallel to objects in the
     * `/[...storeDirectory]/sysmeta/` directory.
     * 
     * @param sysmeta Input stream to metadata document
     * @param pid     Authority-based identifier
     * @return String representing metadata address
     * @throws Exception TODO: Add specific exceptions
     */
    String storeSysmeta(InputStream sysmeta, String pid) throws Exception;

    /**
     * The `retrieveObject` method retrieves an object from disk using a given
     * persistent identifier (pid). If the object exists (determined by calculating
     * the object's permanent address using the SHA-256 hash of the given pid), the
     * method will open and return a buffered object stream ready to read from.
     * 
     * @param pid Authority-based identifier
     * @return A buffered stream of the object
     * @throws Exception TODO: Add specific exceptions
     */
    BufferedReader retrieveObject(String pid) throws Exception;

    /**
     * The 'retrieveSysmeta' method retrieves the metadata content from disk and
     * returns it in the form of a String using a given persistent identifier.
     * 
     * @param pid Authority-based identifier
     * @return Sysmeta (metadata) document of given pid
     * @throws Exception TODO: Add specific exceptions
     */
    String retrieveSysmeta(String pid) throws Exception;

    /**
     * The 'deleteObject' method deletes an object permanently from disk using a
     * given persistent identifier.
     * 
     * @param pid Authority-based identifier
     * @return
     * @throws Exception TODO: Add specific exceptions
     */
    boolean deleteObject(String pid) throws Exception;

    /**
     * The 'deleteSysmeta' method deletes an metadata document (sysmeta) permanently
     * from disk using a given persistent identifier.
     * 
     * @param pid Authority-based identifier
     * @return
     * @throws Exception TODO: Add specific exceptions
     */
    boolean deleteSysmeta(String pid) throws Exception;

    /**
     * The 'getHexDigest' method calculates the hex digest of an object that exists
     * in HashStore using a given persistent identifier and hash algorithm.
     * 
     * @param pid       Authority-based identifier
     * @param algorithm Algorithm of desired hex digest
     * @return
     * @throws Exception TODO: Add specific exceptions
     */
    String getHexDigest(String pid, String algorithm) throws Exception;
}

package org.dataone.hashstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.dataone.hashstore.exceptions.PidObjectExistsException;

/**
 * HashStore is a content-addressable file management system that utilizes a
 * persistent identifier (PID) in the form of a hex digest value to address
 * files. The system stores files in a file store and provides an API for
 * interacting with the store. HashStore storage classes (like `FileHashStore`)
 * must implement the HashStore interface to ensure proper usage of the system.
 */
public interface HashStore {
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
         * @throws NoSuchAlgorithmException When additionalAlgorithm or
         *                                  checksumAlgorithm is invalid
         * @throws IOException              I/O Error when writing file, generating
         *                                  checksums and/or moving file
         * @throws PidObjectExistsException When duplicate pid object is found
         * @throws RuntimeException         Thrown when there is an issue with
         *                                  permissions, illegal arguments (ex.
         *                                  empty pid) or null pointers
         */
        HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
                        String checksumAlgorithm)
                        throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException;

        /**
         * The `storeMetadata` method is responsible for adding/updating metadata
         * (ex. `sysmeta`) to disk using a given InputStream, a persistent identifier
         * (pid) and metadata format (formatId). The metadata object contains solely the
         * given metadata content.
         * 
         * The permanent address of the metadata document is determined by calculating
         * the SHA-256 hex digest of the provided `pid` + `format_id`; and the body
         * contains the metadata content (ex. `sysmeta`).
         * 
         * Upon successful storage of metadata, `storeMetadata` returns a string that
         * represents the path of the file's permanent address, as described above.
         * Lastly, the metadata objects are stored in parallel to objects in the
         * `/store_directory/metadata/` directory.
         * 
         * @param metadata Input stream to metadata document
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @return Metadata content identifier (string representing metadata address)
         * @throws IOException              When there is an error writing the metadata
         *                                  document
         * @throws IllegalArgumentException Invalid values like null for metadata, or
         *                                  empty pids and formatIds
         * @throws FileNotFoundException    When temp metadata file is not found
         * @throws InterruptedException     metadataLockedIds synchronization issue
         * @throws NoSuchAlgorithmException Algorithm used to calculate permanent
         *                                  address is not supported
         */
        String storeMetadata(InputStream metadata, String pid, String formatId)
                        throws IOException, IllegalArgumentException, FileNotFoundException, InterruptedException,
                        NoSuchAlgorithmException;

        /**
         * The `retrieveObject` method retrieves an object from disk using a given
         * persistent identifier (pid). If the object exists (determined by calculating
         * the object's permanent address using the SHA-256 hash of the given pid), the
         * method will open and return a buffered object stream ready to read from.
         * 
         * @param pid Authority-based identifier
         * @return Object InputStream
         * @throws IllegalArgumentException When pid is null or empty
         * @throws FileNotFoundException    When requested pid has no associated object
         * @throws IOException              I/O error when creating InputStream to
         *                                  object
         * @throws NoSuchAlgorithmException When algorithm used to calculate object
         *                                  address is not supported
         */
        InputStream retrieveObject(String pid)
                        throws IllegalArgumentException, FileNotFoundException, IOException, NoSuchAlgorithmException;

        /**
         * The 'retrieveMetadata' method retrieves the metadata content of a given pid
         * and metadata namespace from disk and returns it in the form of a String.
         * 
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @return Metadata InputStream
         * @throws IllegalArgumentException When pid/formatId is null or empty
         * @throws FileNotFoundException    When requested pid+formatId has no
         *                                  associated object
         * @throws IOException              I/O error when creating InputStream to
         *                                  metadata
         * @throws NoSuchAlgorithmException When algorithm used to calculate metadata
         *                                  address is not supported
         */
        InputStream retrieveMetadata(String pid, String formatId) throws Exception;

        /**
         * The 'deleteObject' method deletes an object permanently from disk using a
         * given persistent identifier and any empty subdirectories.
         * 
         * @param pid Authority-based identifier
         * @return True if successful
         * @throws IllegalArgumentException When pid is null or empty
         * @throws FileNotFoundException    When requested pid has no associated object
         * @throws IOException              I/O error when deleting empty directories
         * @throws NoSuchAlgorithmException When algorithm used to calculate object
         *                                  address is not supported
         */
        boolean deleteObject(String pid) throws Exception;

        /**
         * The 'deleteMetadata' method deletes a metadata document (ex. `sysmeta`)
         * permanently from disk using a given persistent identifier and its respective
         * metadata namespace.
         * 
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @return True if successfulÏ
         * @throws IllegalArgumentException When pid or formatId is null or empty
         * @throws FileNotFoundException    When requested pid has no metadata
         * @throws IOException              I/O error when deleting empty directories
         * @throws NoSuchAlgorithmException When algorithm used to calculate object
         *                                  address is not supported
         */
        boolean deleteMetadata(String pid, String formatId) throws Exception;

        /**
         * The 'getHexDigest' method calculates the hex digest of an object that exists
         * in HashStore using a given persistent identifier and hash algorithm.
         * 
         * @param pid       Authority-based identifier
         * @param algorithm Algorithm of desired hex digest
         * @return String hex digest of requested pid
         * @throws IllegalArgumentException When pid or formatId is null or empty
         * @throws FileNotFoundException    When requested pid object does not exist
         * @throws IOException              I/O error when calculating hex digests
         * @throws NoSuchAlgorithmException When algorithm used to calculate object
         *                                  address is not supported
         */
        String getHexDigest(String pid, String algorithm) throws Exception;
}

package org.dataone.hashstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.dataone.hashstore.exceptions.PidObjectExistsException;

/**
 * HashStore is a content-addressable file management system that utilizes the hash/hex digest of a
 * given persistent identifier (PID) to address files. The system stores both objects and metadata
 * in its respective directories and provides an API for interacting with the store. HashStore
 * storage classes (like `FileHashStore`) must implement the HashStore interface to ensure proper
 * usage of the system.
 */
public interface HashStore {
        /**
         * The `storeObject` method is responsible for the atomic storage of objects to HashStore
         * using a given InputStream and a persistent identifier (pid). Upon successful storage, the
         * method returns an 'ObjectInfo' object containing the object's file information, such
         * as the id, file size, and hex digest map of algorithms and hex digests/checksums. An
         * object is stored once and only once - and `storeObject` also enforces this rule by
         * synchronizing multiple calls and rejecting calls to store duplicate objects.
         * 
         * The file's id is determined by calculating the SHA-256 hex digest of the provided pid,
         * which is also used as the permanent address of the file. The file's identifier is then
         * sharded using a depth of 3 and width of 2, delimited by '/' and concatenated to produce
         * the final permanent address, which is stored in the object store directory (ex.
         * `./[storePath]/objects/`).
         * 
         * By default, the hex digest map includes the following hash algorithms: MD5, SHA-1,
         * SHA-256, SHA-384 and SHA-512, which are the most commonly used algorithms in dataset
         * submissions to DataONE and the Arctic Data Center. If an additional algorithm is
         * provided, the `storeObject` method checks if it is supported and adds it to the map along
         * with its corresponding hex digest. An algorithm is considered "supported" if it is
         * recognized as a valid hash algorithm in the `java.security.MessageDigest` class.
         * 
         * Similarly, if a checksum and a checksumAlgorithm or an object size value is provided,
         * `storeObject` validates the object to ensure it matches what is provided before moving
         * the file to its permanent address.
         * 
         * @param object              Input stream to file
         * @param pid                 Authority-based identifier
         * @param additionalAlgorithm Additional hex digest to include in hexDigests
         * @param checksum            Value of checksum to validate against
         * @param checksumAlgorithm   Algorithm of checksum submitted
         * @param objSize             Expected size of object to validate after storing
         * @return ObjectInfo object encapsulating file information
         * @throws NoSuchAlgorithmException When additionalAlgorithm or checksumAlgorithm is invalid
         * @throws IOException              I/O Error when writing file, generating checksums and/or
         *                                  moving file
         * @throws PidObjectExistsException When duplicate pid object is found
         * @throws RuntimeException         Thrown when there is an issue with permissions, illegal
         *                                  arguments (ex. empty pid) or null pointers
         */
        ObjectInfo storeObject(
                InputStream object, String pid, String additionalAlgorithm, String checksum,
                String checksumAlgorithm, long objSize
        ) throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException;

        ObjectInfo storeObject(
                InputStream object, String pid, String checksum, String checksumAlgorithm
        ) throws NoSuchAlgorithmException, IOException, PidObjectExistsException, RuntimeException;

        ObjectInfo storeObject(InputStream object, String pid, String additionalAlgorithm)
                throws NoSuchAlgorithmException, IOException, PidObjectExistsException,
                RuntimeException;

        ObjectInfo storeObject(InputStream object, String pid, long objSize)
                throws NoSuchAlgorithmException, IOException, PidObjectExistsException,
                RuntimeException;

        /**
         * The `storeMetadata` method is responsible for adding/updating metadata (ex. `sysmeta`) to
         * the HashStore by using a given InputStream, a persistent identifier (`pid`) and metadata
         * format (`formatId`). The permanent address of the stored metadata document is determined
         * by calculating the SHA-256 hex digest of the provided `pid` + `formatId`.
         * 
         * Note, multiple calls to store the same metadata content will all be accepted, but is not
         * guaranteed to execute sequentially.
         * 
         * @param metadata Input stream to metadata document
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @return Metadata content identifier (string representing metadata address)
         * @throws IOException              When there is an error writing the metadata document
         * @throws IllegalArgumentException Invalid values like null for metadata, or empty pids and
         *                                  formatIds
         * @throws FileNotFoundException    When temp metadata file is not found
         * @throws InterruptedException     metadataLockedIds synchronization issue
         * @throws NoSuchAlgorithmException Algorithm used to calculate permanent address is not
         *                                  supported
         */
        String storeMetadata(InputStream metadata, String pid, String formatId) throws IOException,
                IllegalArgumentException, FileNotFoundException, InterruptedException,
                NoSuchAlgorithmException;

        String storeMetadata(InputStream metadata, String pid) throws IOException,
                IllegalArgumentException, FileNotFoundException, InterruptedException,
                NoSuchAlgorithmException;

        /**
         * The `retrieveObject` method retrieves an object from HashStore using a given persistent
         * identifier (pid).
         * 
         * @param pid Authority-based identifier
         * @return Object InputStream
         * @throws IllegalArgumentException When pid is null or empty
         * @throws FileNotFoundException    When requested pid has no associated object
         * @throws IOException              I/O error when creating InputStream to object
         * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
         *                                  supported
         */
        InputStream retrieveObject(String pid) throws IllegalArgumentException,
                FileNotFoundException, IOException, NoSuchAlgorithmException;

        /**
         * The 'retrieveMetadata' method retrieves the metadata content of a given pid and metadata
         * namespace from HashStore.
         * 
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @return Metadata InputStream
         * @throws IllegalArgumentException When pid/formatId is null or empty
         * @throws FileNotFoundException    When requested pid+formatId has no associated object
         * @throws IOException              I/O error when creating InputStream to metadata
         * @throws NoSuchAlgorithmException When algorithm used to calculate metadata address is not
         *                                  supported
         */
        InputStream retrieveMetadata(String pid, String formatId) throws Exception;

        /**
         * The 'deleteObject' method deletes an object (and its empty subdirectories) permanently
         * from HashStore using a given persistent identifier.
         * 
         * @param pid Authority-based identifier
         * @throws IllegalArgumentException When pid is null or empty
         * @throws FileNotFoundException    When requested pid has no associated object
         * @throws IOException              I/O error when deleting empty directories
         * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
         *                                  supported
         */
        void deleteObject(String pid) throws Exception;

        /**
         * The 'deleteMetadata' method deletes a metadata document (ex. `sysmeta`) permanently from
         * HashStore using a given persistent identifier and its respective metadata namespace.
         * 
         * @param pid      Authority-based identifier
         * @param formatId Metadata namespace/format
         * @throws IllegalArgumentException When pid or formatId is null or empty
         * @throws FileNotFoundException    When requested pid has no metadata
         * @throws IOException              I/O error when deleting empty directories
         * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
         *                                  supported
         */
        void deleteMetadata(String pid, String formatId) throws Exception;

        /**
         * The 'getHexDigest' method calculates the hex digest of an object that exists in HashStore
         * using a given persistent identifier and hash algorithm.
         * 
         * @param pid       Authority-based identifier
         * @param algorithm Algorithm of desired hex digest
         * @return String hex digest of requested pid
         * @throws IllegalArgumentException When pid or formatId is null or empty
         * @throws FileNotFoundException    When requested pid object does not exist
         * @throws IOException              I/O error when calculating hex digests
         * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
         *                                  supported
         */
        String getHexDigest(String pid, String algorithm) throws Exception;
}

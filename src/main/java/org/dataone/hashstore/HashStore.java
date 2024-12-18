package org.dataone.hashstore;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import org.dataone.hashstore.exceptions.NonMatchingChecksumException;
import org.dataone.hashstore.exceptions.NonMatchingObjSizeException;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.exceptions.UnsupportedHashAlgorithmException;

/**
 * HashStore is a content-addressable file management system that utilizes the content identifier of
 * an object to address files. The system stores both objects, references (refs) and metadata in its
 * respective directories and provides an API for interacting with the store. HashStore storage
 * classes (like {@code FileHashStore}) must implement the HashStore interface to ensure the
 * expected usage of the system.
 */
public interface HashStore {
    /**
     * The {@code storeObject} method is responsible for the atomic storage of objects to disk using
     * a given InputStream. Upon successful storage, the method returns an (@Code ObjectMetadata)
     * object containing relevant file information, such as the file's id (which can be used by a
     * system administrator -- but not by an API client -- to locate the object on disk), the file's
     * size, and a hex digest dict of algorithms and checksums. Storing an object with
     * {@code store_object} also tags an object (creating references) which allow the object to be
     * discoverable.
     *
     * {@code storeObject} also ensures that an object is stored only once by synchronizing multiple
     * calls and rejecting calls to store duplicate objects. Note, calling {@code storeObject}
     * without a pid is a possibility, but should only store the object without tagging the object.
     * It is then the caller's responsibility to finalize the process by calling {@code tagObject}
     * after verifying the correct object is stored.
     *
     * The file's id is determined by calculating the object's content identifier based on the
     * store's default algorithm, which is also used as the permanent address of the file. The
     * file's identifier is then sharded using the store's configured depth and width, delimited by
     * '/' and concatenated to produce the final permanent address and is stored in the
     * {@code ./[storePath]/objects/} directory.
     *
     * By default, the hex digest map includes the following hash algorithms: MD5, SHA-1, SHA-256,
     * SHA-384, SHA-512 - which are the most commonly used algorithms in dataset submissions to
     * DataONE and the Arctic Data Center. If an additional algorithm is provided, the
     * {@code storeObject} method checks if it is supported and adds it to the hex digests dict
     * along with its corresponding hex digest. An algorithm is considered "supported" if it is
     * recognized as a valid hash algorithm in {@code java.security .MessageDigest} class.
     *
     * Similarly, if a file size and/or checksum & checksumAlgorithm value are provided,
     * {@code storeObject} validates the object to ensure it matches the given arguments before
     * moving the file to its permanent address.
     *
     * @param object              Input stream to file
     * @param pid                 Authority-based identifier
     * @param additionalAlgorithm Additional hex digest to include in hexDigests
     * @param checksum            Value of checksum to validate against
     * @param checksumAlgorithm   Algorithm of checksum submitted
     * @param objSize             Expected size of object to validate after storing
     * @return ObjectMetadata object encapsulating file information
     * @throws NoSuchAlgorithmException   When additionalAlgorithm or checksumAlgorithm is invalid
     * @throws IOException                I/O Error when writing file, generating checksums and/or
     *                                    moving file
     * @throws PidRefsFileExistsException If a pid refs file already exists, meaning the pid is
     *                                    already referencing a file.
     * @throws RuntimeException           Thrown when there is an issue with permissions, illegal
     *                                    arguments (ex. empty pid) or null pointers
     * @throws InterruptedException       When tagging pid and cid process is interrupted
     */
    ObjectMetadata storeObject(
        InputStream object, String pid, String additionalAlgorithm, String checksum,
        String checksumAlgorithm, long objSize)
        throws NoSuchAlgorithmException, IOException, PidRefsFileExistsException, RuntimeException,
        InterruptedException;

    /**
     * @see #storeObject(InputStream, String, String, String, String, long)
     *
     *     Store an object only without reference files.
     */
    ObjectMetadata storeObject(InputStream object)
        throws NoSuchAlgorithmException, IOException, RuntimeException,
        InterruptedException;

    /**
     * Creates references that allow objects stored in HashStore to be discoverable. Retrieving,
     * deleting or calculating a hex digest of an object is based on a pid argument; and to proceed,
     * we must be able to find the object associated with the pid.
     *
     * @param pid Authority-based identifier
     * @param cid Content-identifier (hash identifier)
     * @throws IOException                Failure to create tmp file
     * @throws PidRefsFileExistsException When pid refs file already exists
     * @throws NoSuchAlgorithmException   When algorithm used to calculate pid refs address does not
     *                                    exist
     * @throws FileNotFoundException      If refs file is missing during verification
     * @throws InterruptedException       When tagObject is waiting to execute but is interrupted
     */
    void tagObject(String pid, String cid)
        throws IOException, PidRefsFileExistsException, NoSuchAlgorithmException,
        FileNotFoundException, InterruptedException;

    /**
     * Confirms that an ObjectMetadata's content is equal to the given values. This method throws an
     * exception if there are any issues, and attempts to remove the data object if it is determined
     * to be invalid.
     *
     * @param objectInfo        ObjectMetadata object with values
     * @param checksum          Value of checksum to validate against
     * @param checksumAlgorithm Algorithm of checksum submitted
     * @param objSize           Expected size of object to validate after storing
     * @throws NonMatchingObjSizeException       Given size =/= objMeta size value
     * @throws NonMatchingChecksumException      Given checksum =/= objMeta checksum value
     * @throws UnsupportedHashAlgorithmException Given algo is not found or supported
     * @throws NoSuchAlgorithmException          When 'deleteInvalidObject' is true and an algo used
     *                                           to get a cid refs file is not supported
     * @throws InterruptedException              When 'deleteInvalidObject' is true and an issue
     *                                           with coordinating deleting objects occurs
     * @throws IOException                       Issue with recalculating supported algo for
     *                                           checksum not found
     */
    void deleteIfInvalidObject(
        ObjectMetadata objectInfo, String checksum, String checksumAlgorithm, long objSize)
        throws NonMatchingObjSizeException, NonMatchingChecksumException,
        UnsupportedHashAlgorithmException, InterruptedException, NoSuchAlgorithmException,
        IOException;

    /**
     * Adds/updates metadata (ex. {@code sysmeta}) to the HashStore by using a given InputStream, a
     * persistent identifier ({@code pid}) and metadata format ({@code formatId}). All metadata
     * documents for a given pid will be stored in the directory (under ../metadata) that is
     * determined by calculating the hash of the given pid, with the document name being the hash of
     * the metadata format ({@code formatId}).
     *
     * Note, multiple calls to store the same metadata content will all be accepted, but is not
     * guaranteed to execute sequentially.
     *
     * @param metadata Input stream to metadata document
     * @param pid      Authority-based identifier
     * @param formatId Metadata namespace/format
     * @return Path to metadata content identifier (string representing metadata address)
     * @throws IOException              When there is an error writing the metadata document
     * @throws IllegalArgumentException Invalid values like null for metadata, or empty pids and
     *                                  formatIds
     * @throws FileNotFoundException    When temp metadata file is not found
     * @throws InterruptedException     metadataLockedIds synchronization issue
     * @throws NoSuchAlgorithmException Algorithm used to calculate permanent address is not
     *                                  supported
     */
    String storeMetadata(InputStream metadata, String pid, String formatId)
        throws IOException, IllegalArgumentException, FileNotFoundException, InterruptedException,
        NoSuchAlgorithmException;

    /**
     * @see #storeMetadata(InputStream, String, String)
     *
     *     If the '(InputStream metadata, String pid)' signature is used, the metadata format stored
     *     will default to {@code sysmeta}.
     */
    String storeMetadata(InputStream metadata, String pid)
        throws IOException, IllegalArgumentException, InterruptedException,
        NoSuchAlgorithmException;

    /**
     * Returns an InputStream to an object from HashStore using a given persistent identifier.
     *
     * @param pid Authority-based identifier
     * @return Object InputStream
     * @throws IllegalArgumentException When pid is null or empty
     * @throws FileNotFoundException    When requested pid has no associated object
     * @throws IOException              I/O error when creating InputStream to object
     * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
     *                                  supported
     */
    InputStream retrieveObject(String pid)
        throws IllegalArgumentException, FileNotFoundException, IOException,
        NoSuchAlgorithmException;

    /**
     * Returns an InputStream to the metadata content of a given pid and metadata namespace from
     * HashStore.
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
    InputStream retrieveMetadata(String pid, String formatId)
        throws IllegalArgumentException, FileNotFoundException, IOException,
        NoSuchAlgorithmException;

    /**
     * @see #retrieveMetadata(String, String)
     *
     *     If {@code retrieveMetadata} is called with signature (String pid), the metadata document
     *     retrieved will be the given pid's 'sysmeta'
     */
    InputStream retrieveMetadata(String pid)
        throws IllegalArgumentException, IOException,
        NoSuchAlgorithmException;

    /**
     * Deletes an object and all relevant associated files (ex. system metadata, reference files,
     * etc.) based on a given pid. If other pids still reference the pid's associated object, the
     * object will not be deleted.
     *
     * @param pid Authority-based identifier
     * @throws IllegalArgumentException When pid is null or empty
     * @throws IOException              I/O error when deleting empty directories,
     *                                  modifying/deleting reference files
     * @throws NoSuchAlgorithmException When algorithm used to calculate an object or metadata's
     *                                  address is not supported
     * @throws InterruptedException     When deletion synchronization is interrupted
     */
    void deleteObject(String pid)
        throws IllegalArgumentException, IOException, NoSuchAlgorithmException,
        InterruptedException;

    /**
     * Deletes a metadata document (ex. {@code sysmeta}) permanently from HashStore using a given
     * persistent identifier and its respective metadata namespace.
     *
     * @param pid      Authority-based identifier
     * @param formatId Metadata namespace/format
     * @throws IllegalArgumentException When pid or formatId is null or empty
     * @throws IOException              I/O error when deleting metadata or empty directories
     * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
     *                                  supported
     * @throws InterruptedException     Issue with synchronization on metadata doc
     */
    void deleteMetadata(String pid, String formatId)
        throws IllegalArgumentException, IOException, NoSuchAlgorithmException,
        InterruptedException;

    /**
     * Deletes all metadata related for the given 'pid' from HashStore
     *
     * @param pid Authority-based identifier
     * @throws IllegalArgumentException If pid is invalid
     * @throws IOException              I/O error when deleting metadata or empty directories
     * @throws NoSuchAlgorithmException When algorithm used to calculate object address is not
     *                                  supported
     * @throws InterruptedException     Issue with synchronization on metadata doc
     */
    void deleteMetadata(String pid)
        throws IllegalArgumentException, IOException, NoSuchAlgorithmException,
        InterruptedException;

    /**
     * Calculates the hex digest of an object that exists in HashStore using a given persistent
     * identifier and hash algorithm.
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
    String getHexDigest(String pid, String algorithm)
        throws IllegalArgumentException, FileNotFoundException, IOException,
        NoSuchAlgorithmException;
}

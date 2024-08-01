package org.dataone.hashstore.hashstoreconverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * HashStoreConverter is a utility tool to assist with ingesting existing data objects and their
 * respective system metadata into a HashStore. Instead of duplicating data objects (that already
 * exists), HashStoreConverter provide a simple API to create a hard link to the data object with
 * the expected HashStore object path.
 */
public class HashStoreConverter {
    private static final Log logHashStoreConverter = LogFactory.getLog(HashStoreConverter.class);
    private FileHashStoreLinks fileHashStoreLinks;

    /**
     * Constructor to initialize HashStoreConverter. Properties to an existing or desired HashStore
     * are required.
     *
     * @param hashstoreProperties Properties object with the following keys: storePath, storeDepth,
     *                            storeWidth, storeAlgorithm, storeMetadataNamespace
     * @throws IOException              Issue with directories or hashstore.yaml config
     * @throws NoSuchAlgorithmException Unsupported FileHashStoreLinks algorithm
     */
    public HashStoreConverter(Properties hashstoreProperties)
        throws IOException, NoSuchAlgorithmException {
        try {
            fileHashStoreLinks = new FileHashStoreLinks(hashstoreProperties);
            logHashStoreConverter.info("HashStoreConverter initialized");

        } catch (IOException ioe) {
            String errMsg = "Unexpected IOException encountered: " + ioe.getMessage();
            logHashStoreConverter.error(errMsg);
            throw ioe;

        } catch (NoSuchAlgorithmException nsae) {
            String errMsg = "A supplied algorithm is not supported: " + nsae.getMessage();
            logHashStoreConverter.error(errMsg);
            throw nsae;

        }
    }

    /**
     * Take an existing path to a data object, store it into a new or existing HashStore via a
     * hard link (to save disk space), store the supplied system metadata and return the
     * ObjectMetadata for the data object. The 'sysMetaStream' given may never be null.
     * A 'filePath' may be null, in which case a data object will not be stored.
     *
     * @param filePath      Path to existing data object
     * @param pid           Persistent or authority-based identifier
     * @param sysmetaStream Stream to sysmeta content to store.
     * @param checksum   Value of checksum
     * @param checksumAlgorithm Ex. "SHA-256"
     * @return ObjectMetadata for the given pid
     * @throws IOException              An issue with calculating checksums or storing sysmeta
     * @throws NoSuchAlgorithmException An algorithm defined is not supported
     * @throws InterruptedException     Issue with synchronizing storing metadata
     */
    public ObjectMetadata convert(Path filePath, String pid, InputStream sysmetaStream,
                                  String checksum, String checksumAlgorithm)
        throws IOException, NoSuchAlgorithmException, InterruptedException {
        logHashStoreConverter.info("Begin converting data object and sysmeta for pid: " + pid);
        FileHashStoreUtility.ensureNotNull(sysmetaStream, "sysmetaStream", "convert");
        FileHashStoreUtility.ensureNotNull(pid, "pid", "convert");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "convert");
        FileHashStoreUtility.ensureNotNull(checksum, "checksum", "convert");
        FileHashStoreUtility.checkForEmptyAndValidString(checksum, "checksum", "convert");
        FileHashStoreUtility.ensureNotNull(checksumAlgorithm, "checksumAlgorithm", "convert");
        FileHashStoreUtility.checkForEmptyAndValidString(
            checksumAlgorithm, "checksumAlgorithm", "convert");

        // Store the sysmeta first - this can never be null and is always required.
        try {
            fileHashStoreLinks.storeMetadata(sysmetaStream, pid);
        } finally {
            sysmetaStream.close();
        }

        // Now store the hard link, which is optional
        ObjectMetadata objInfo = null;
        if (filePath != null) {
            try (InputStream fileStream = Files.newInputStream(filePath)) {
                objInfo = fileHashStoreLinks.storeHardLink(filePath, fileStream, pid, checksum,
                                                           checksumAlgorithm);
                logHashStoreConverter.info("Stored data object for pid: " + pid);

            } catch (IOException ioe) {
                String errMsg = "Unexpected IOException encountered: " + ioe.getMessage();
                logHashStoreConverter.error(errMsg);
                throw ioe;

            } catch (NoSuchAlgorithmException nsae) {
                String errMsg = "A supplied algorithm is not supported: " + nsae.getMessage();
                logHashStoreConverter.error(errMsg);
                throw nsae;

            } catch (InterruptedException ie) {
                String errMsg =
                    "Unexpected issue with synchronizing storing data objects or metadata: "
                        + ie.getMessage();
                logHashStoreConverter.error(errMsg);
                throw ie;
            }
        } else {
            String warnMsg = "Supplied filePath is null, not storing data object.";
            logHashStoreConverter.warn(warnMsg);
        }

        return objInfo;
    }
}

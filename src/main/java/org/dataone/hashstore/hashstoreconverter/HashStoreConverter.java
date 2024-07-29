package org.dataone.hashstore.hashstoreconverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

/**
 * HashStoreConverter is a utility tool to assist with ingesting existing data objects and their
 * respective system metadata into a HashStore. Instead of duplicating data objects and writing
 * ObjectMetadata.
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
            logHashStoreConverter.info("FileHashStoreLinks initialized");

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
     * ObjectMetadata for the data object.
     *
     * @param filePath      Path to existing data object
     * @param pid           Persistent or authority-based identifier
     * @param sysmetaStream Stream to sysmeta content to store
     * @return ObjectMetadata for the given pid
     * @throws IOException              An issue with calculating checksums or storing sysmeta
     * @throws NoSuchAlgorithmException An algorithm defined is not supported
     * @throws InterruptedException     Issue with synchronizing storing metadata
     */
    public ObjectMetadata convert(Path filePath, String pid, InputStream sysmetaStream)
        throws IOException, NoSuchAlgorithmException, InterruptedException {
        logHashStoreConverter.info("Begin converting data object and sysmeta for pid: " + pid);

        try (InputStream fileStream = Files.newInputStream(filePath)) {
            ObjectMetadata objInfo = fileHashStoreLinks.storeHardLink(filePath, fileStream, pid);
            fileHashStoreLinks.storeMetadata(sysmetaStream, pid);
            return objInfo;

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

        } finally {
            sysmetaStream.close();
        }
    }
}

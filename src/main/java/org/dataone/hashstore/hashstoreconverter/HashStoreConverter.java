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

public class HashStoreConverter {
    private static final Log logHashStoreConverter = LogFactory.getLog(HashStoreConverter.class);
    private FileHashStoreLinks fileHashStoreLinks;

    /**
     * Properties to an existing HashStore are required to initialize HashStoreConverter.
     * HashStoreConverter is a utility tool to assist clients to convert
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
            String errMsg = "Unexpected issue with an algorithm encountered: " + nsae.getMessage();
            logHashStoreConverter.error(errMsg);
            throw nsae;

        }
    }

    // TODO Finish Javadocs
    /**
     * Create a hard link in the specified hashstore for an existing data object.
     *
     * @param filePath Path to existing data object
     * @param pid Persistent or authority-based identifier
     * @param sysmetaStream Stream to sysmeta content to store
     * @return
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws InterruptedException
     */
    public ObjectMetadata convert(Path filePath, String pid, InputStream sysmetaStream)
        throws IOException, NoSuchAlgorithmException, InterruptedException {
        // TODO Review flow
        // TODO Add junit tests

        try {
            InputStream fileStream = Files.newInputStream(filePath);
            ObjectMetadata objInfo = fileHashStoreLinks.storeHardLink(filePath, fileStream, pid);
            fileHashStoreLinks.storeMetadata(sysmetaStream, pid);
            return objInfo;
        } catch (IOException ioe) {
            throw ioe;
            // TODO
        } catch (NoSuchAlgorithmException nsae) {
            throw nsae;
            // TODO
        } catch (InterruptedException ie) {
            throw ie;
            // TODO
        }
    }
}

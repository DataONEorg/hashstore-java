package org.dataone.hashstore;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.filehashstore.FileHashStore;

/**
 * HashStoreFactory is a factory class that generates HashStore, a
 * content-addressable file management system.
 */
public class HashStoreFactory {
    private static final Log logHashStore = LogFactory.getLog(HashStoreFactory.class);

    /**
     * Factory method to generate a Hashstore
     * 
     * @param store_type Ex. "filehashstore"
     * @throws HashStoreFactoryException Factory method cannot instantiate a store
     * @throws IOException               When properties cannot be retrieved
     */
    public static HashStore getHashStore(String store_type) throws HashStoreFactoryException, IOException {
        int hashstore_depth;
        int hashstore_width;
        String hashstore_algorithm;
        Path hashstore_path;

        try {
            // Get properties
            Properties fhsProperties = new Properties();
            String fileName = "HashStore.properties";
            URL resourceUrl = HashStore.class.getClassLoader().getResource(fileName);
            InputStream inputStream = resourceUrl.openStream();
            fhsProperties.load(inputStream);

            // Get depth, width and hashing algorithm for permanent address
            hashstore_depth = Integer.parseInt(fhsProperties.getProperty("filehashstore.depth"));
            hashstore_width = Integer.parseInt(fhsProperties.getProperty("filehashstore.width"));
            hashstore_algorithm = fhsProperties.getProperty("filehashstore.algorithm");

            // Get path of store, create parent folder if it doesn't already exist
            hashstore_path = Paths.get(fhsProperties.getProperty("filehashstore.storepath"));

        } catch (NullPointerException npe) {
            logHashStore.error(
                    "HashStoreFactory - Cannot configure FileHashStore. Properties file is null: "
                            + npe.getMessage());
            throw new HashStoreFactoryException("Properties file not found (null). " + npe.getMessage());
        } catch (IOException ioe) {
            logHashStore.error(
                    "HashStoreFactory - Cannot configure FileHashStore. Error reading properties file: "
                            + ioe.getMessage());
            throw new HashStoreFactoryException("Unable to load properties. " + ioe.getMessage());
        }

        // Get HashStore
        HashStore hashstore = null;
        store_type.toLowerCase();
        if (store_type.equals("filehashstore")) {
            logHashStore.debug("Creating new 'FileHashStore' hashstore");
            try {
                hashstore = new FileHashStore(hashstore_depth, hashstore_width, hashstore_algorithm, hashstore_path);
            } catch (IOException ioe) {
                logHashStore.error("HashStoreFactory - Unable to generate 'filehashstore'. " + ioe.getMessage());
                throw new HashStoreFactoryException(
                        "Unable to generate 'filehashstore' HashStore. " + ioe.getMessage());
            }
        }
        return hashstore;
    }

}

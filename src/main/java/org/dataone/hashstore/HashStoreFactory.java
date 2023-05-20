package org.dataone.hashstore;

import java.io.FileInputStream;
import java.io.IOException;
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
        int hashstore_depth = 0;
        int hashstore_width = 0;
        String hashstore_algorithm = "";
        Path hashstore_path = Paths.get("Test");
        // Get properties
        Properties fhsProperties = new Properties();
        String fileName = "FileHashStore.properties";
        URL resourceUrl = FileHashStore.class.getResource(fileName);
        try {
            FileInputStream fileInputStream = new FileInputStream(resourceUrl.getPath());
            fhsProperties.load(fileInputStream);

            // Get depth, width and hashing algorithm for permanent address
            hashstore_depth = Integer.parseInt(fhsProperties.getProperty("filehashstore.depth"));
            hashstore_width = Integer.parseInt(fhsProperties.getProperty("filehashstore.width"));
            hashstore_algorithm = fhsProperties.getProperty("filehashstore.algorithm");

            // Get path of store, create parent folder if it doesn't already exist
            hashstore_path = Paths.get(fhsProperties.getProperty("filehashstore.storepath"));

            // Get and set default and supported hash algorithm property values
            String[] default_algorithm_list = fhsProperties.getProperty("filehashstore.default_algorithms").split(",");
            String[] supported_algorithm_list = fhsProperties.getProperty("filehashstore.supported_algorithms")
                    .split(",");
        } catch (IOException e) {
            logHashStore.error(
                    "HashStoreFactory - Cannot configure FileHashStore. Error reading properties file: "
                            + e.getMessage());
        }

        // Get HashStore
        HashStore hashstore = null;
        store_type.toLowerCase();
        if (store_type == "filehashstore") {
            logHashStore.debug("Creating new 'FileHashStore' hashstore");
            try {
                hashstore = new FileHashStore(hashstore_depth, hashstore_width, hashstore_algorithm, hashstore_path);
            } catch (IOException ioe) {
                logHashStore.error("HashStoreFactory - Unable to generate 'filehashstore'. " + ioe.getMessage());
                throw new HashStoreFactoryException(ioe.getMessage());
            }
        }
        return hashstore;
    }

}

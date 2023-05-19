package org.dataone.hashstore;

import java.io.IOException;
import java.nio.file.Path;

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
     * @param store_type Type of store desired (ex. "FileHashStore")
     * @return
     */
    public static HashStore getHashStore(String store_type) {
        HashStore hashstore = null;
        store_type.toLowerCase();
        if (store_type == "filehashstore") {
            logHashStore.debug("Creating new 'FileHashStore' hashstore");
            try {
                hashstore = new FileHashStore();
            } catch (HashStoreFactoryException hsfe) {
                logHashStore.error("HashStoreFactory - Unable to generate 'filehashstore'. " + hsfe.getMessage());
                throw hsfe;
            }
        }
        return hashstore;
    }

}

package org.dataone.hashstore;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.exceptions.HashStoreFactoryException;

/**
 * HashStoreFactory is a factory class that generates HashStore, a
 * content-addressable file management system.
 */
public class HashStoreFactory {
    private static final Log logHashStore = LogFactory.getLog(HashStoreFactory.class);

    /**
     * Factory method to generate a Hashstore
     * 
     * @param classPackage    String of the package name, ex.
     *                        "org.dataone.hashstore.filehashstore.FileHashStore"
     * @param storeProperties HashMap of the HashStore required properties:
     *                        (Path) storePath, (int) storeDepth, (int) StoreWidth,
     *                        (int)
     *                        storeAlgorithm
     * 
     * @return
     * @throws HashStoreFactoryException When HashStore fails to initialize due to
     *                                   permissions or class-related issues
     * @throws IOException               When there is an issue with properties
     */
    public static HashStore getHashStore(String classPackage, HashMap<String, Object> storeProperties)
            throws HashStoreFactoryException, IOException {
        // Validate input parameters
        if (classPackage == null || classPackage.trim().isEmpty()) {
            logHashStore.error("HashStoreFactory - classPackage cannot be null or empty.");
            throw new HashStoreFactoryException("HashStoreFactory - classPackage cannot be null or empty.");

        }
        if (storeProperties == null) {
            logHashStore.error("HashStoreFactory - storeProperties cannot be null.");
            throw new HashStoreFactoryException("HashStoreFactory - storeProperties cannot be null.");

        }

        // Get HashStore
        logHashStore.debug("Creating new 'HashStore' from package: " + classPackage);
        HashStore hashstore;
        try {
            Class<?> hashStoreClass = Class.forName(classPackage);
            Constructor<?> constructor = hashStoreClass.getConstructor(HashMap.class);
            hashstore = (HashStore) constructor.newInstance(storeProperties);

        } catch (ClassNotFoundException cnfe) {
            logHashStore.error("HashStoreFactory - Unable to find 'FileHashStore' class: " + cnfe.getMessage());
            throw new HashStoreFactoryException("Unable to find 'FileHashStore' class: " + cnfe.getMessage());

        } catch (NoSuchMethodException nsme) {
            logHashStore
                    .error("HashStoreFactory - Constructor not found for 'FileHashStore': " + nsme.getMessage());
            throw new HashStoreFactoryException("Constructor not found for 'FileHashStore': " + nsme.getMessage());

        } catch (IllegalAccessException iae) {
            logHashStore
                    .error("HashStoreFactory - Illegal Access Exception encountered: " + iae.getMessage());
            throw new HashStoreFactoryException(
                    "HashStoreFactory - Executing method does not have access to the definition of the"
                            + "specified class , field, method or constructor. Illegal Access Exception encountered:"
                            + iae.getMessage());

        } catch (InstantiationException ie) {
            logHashStore.error(
                    "HashStoreFactory - Error instantiating 'FileHashStore': " + ie.getMessage());
            throw new HashStoreFactoryException(
                    "Error instantiating 'FileHashStore' (likely related to `.newInstance()`): " + ie.getMessage());

        } catch (InvocationTargetException ite) {
            logHashStore.error("HashStoreFactory - InvocationTargetException encountered: " + ite.getMessage()
                    + ". Cause: " + ite.getCause());
            throw new HashStoreFactoryException(
                    "Error creating 'FileHashStore' instance: " + ite.getMessage() + ". Cause: " + ite.getCause());
        }
        return hashstore;
    }
}

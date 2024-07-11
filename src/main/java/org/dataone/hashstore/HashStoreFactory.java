package org.dataone.hashstore;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.exceptions.HashStoreFactoryException;

/**
 * HashStoreFactory is a factory class that generates HashStore, a content-addressable file
 * management system.
 */
public class HashStoreFactory {
    private static final Log logHashStore = LogFactory.getLog(HashStoreFactory.class);

    /**
     * Factory method to generate a HashStore
     * 
     * @param classPackage    String of the package name, ex.
     *                        "org.dataone.hashstore.filehashstore.FileHashStore"
     * @param storeProperties Properties object with the following keys: storePath, storeDepth,
     *                        storeWidth, storeAlgorithm, storeMetadataNamespace
     * 
     * @return HashStore instance ready to store objects and metadata
     * @throws HashStoreFactoryException When HashStore failÏs to initialize due to permissions or
     *                                   class-related issues
     * @throws IOException               When there is an issue with properties
     */
    public static HashStore getHashStore(String classPackage, Properties storeProperties)
        throws HashStoreFactoryException, IOException {
        // Validate input parameters
        if (classPackage == null || classPackage.trim().isEmpty()) {
            String errMsg = "HashStoreFactory - classPackage cannot be null or empty.";
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);
        }
        if (storeProperties == null) {
            String errMsg = "HashStoreFactory - storeProperties cannot be null.";
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);
        }

        // Get HashStore
        logHashStore.debug("Creating new 'HashStore' from package: " + classPackage);
        HashStore hashstore;
        try {
            Class<?> hashStoreClass = Class.forName(classPackage);
            Constructor<?> constructor = hashStoreClass.getConstructor(Properties.class);
            hashstore = (HashStore) constructor.newInstance(storeProperties);

        } catch (ClassNotFoundException cnfe) {
            String errMsg = "HashStoreFactory - Unable to find 'FileHashStore' classPackage: "
                + classPackage + " - " + cnfe.getCause();
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);

        } catch (NoSuchMethodException nsme) {
            String errMsg = "HashStoreFactory - Constructor not found for 'FileHashStore': "
                + classPackage + " - " + nsme.getCause();
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);

        } catch (IllegalAccessException iae) {
            String errMsg =
                "HashStoreFactory - Executing method does not have access to the definition of"
                    + " the specified class , field, method or constructor. " + iae
                        .getCause();
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);

        } catch (InstantiationException ie) {
            String errMsg = "HashStoreFactory - Error instantiating 'FileHashStore'"
                + "(likely related to `.newInstance()`): " + ie.getCause();
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);

        } catch (InvocationTargetException ite) {
            String errMsg = "HashStoreFactory - Error creating 'FileHashStore' instance: " + ite.getCause();
            logHashStore.error(errMsg);
            throw new HashStoreFactoryException(errMsg);

        }
        return hashstore;
    }
}

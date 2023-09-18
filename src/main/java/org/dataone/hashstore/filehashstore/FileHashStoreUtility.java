package org.dataone.hashstore.filehashstore;

/**
 * FileHashStoreUtility is a utility class that provides shared functionality between FileHashStore
 * and related classes.
 */
public class FileHashStoreUtility {
    /**
     * Checks whether a given object is null and throws an exception if so
     *
     * @param object   Object to check
     * @param argument Value that is being checked
     * @param method   Calling method or class
     */
    public static void ensureNotNull(Object object, String argument, String method) {
        if (object == null) {
            String errMsg = "FileHashStoreUtility.isStringNullOrEmpty - Calling Method: " + method
                + "(): " + argument + " cannot be null.";
            throw new NullPointerException(errMsg);
        }
    }
}

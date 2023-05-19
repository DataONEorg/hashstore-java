package org.dataone.hashstore.exceptions;

/**
 * Custom exception class for HashStoreFactory
 */
public class HashStoreFactoryException extends Exception {
    public HashStoreFactoryException(String message) {
        super(message);
    }

    public HashStoreFactoryException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public HashStoreFactoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public HashStoreFactoryException(Throwable cause) {
        super(cause);
    }
}

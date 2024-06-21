package org.dataone.hashstore.exceptions;

/**
 * An exception that encapsulates errors from the HashStore Runnable Test Class
 */
public class HashStoreServiceException extends Exception {
    public HashStoreServiceException(String message) {
        super(message);
    }
}
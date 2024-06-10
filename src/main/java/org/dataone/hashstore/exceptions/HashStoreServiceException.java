package org.dataone.hashstore.exceptions;

/**
 * An exception that encapsulates errors from the HashStore service
 */
public class HashStoreServiceException extends Exception {
    public HashStoreServiceException(String message) {
        super(message);
    }
}
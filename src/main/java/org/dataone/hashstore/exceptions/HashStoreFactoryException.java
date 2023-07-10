package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for HashStoreFactory when it's unable to initialize
 * (like when properties are unavailable or configuration is missing).
 */
public class HashStoreFactoryException extends IOException {
    public HashStoreFactoryException(String message) {
        super(message);
    }

}

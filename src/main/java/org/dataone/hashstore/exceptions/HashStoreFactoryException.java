package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for HashStoreFactory when it's unable to initialize
 * because properties are unavailable or fault.
 */
public class HashStoreFactoryException extends IOException {
    public HashStoreFactoryException(String message) {
        super(message);
    }

    public HashStoreFactoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public HashStoreFactoryException(Throwable cause) {
        super(cause);
    }
}

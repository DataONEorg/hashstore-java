package org.dataone.hashstore.exceptions;

/**
 * An exception thrown when a given algorithm is not supported by FileHashStore java
 */

public class UnsupportedHashAlgorithmException extends IllegalArgumentException {

    public UnsupportedHashAlgorithmException(String message) {
        super(message);
    }

}

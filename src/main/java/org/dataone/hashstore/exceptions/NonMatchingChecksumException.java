package org.dataone.hashstore.exceptions;

/**
 * An exception thrown when a checksum does not match what is expected.
 */

public class NonMatchingChecksumException extends IllegalArgumentException {

    public NonMatchingChecksumException(String message) {
        super(message);
    }

}

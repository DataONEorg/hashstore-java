package org.dataone.hashstore.exceptions;

/**
 * This exception is thrown when an identifier is not locked, breaking thread safety.
 */
public class IdentifierNotLockedException extends RuntimeException {

    public IdentifierNotLockedException(String message) {
        super(message);
    }

}

package org.dataone.hashstore.exceptions;

/**
 * An exception thrown when a data object size does not match what is expected.
 */

public class NonMatchingObjSizeException extends IllegalArgumentException {

    public NonMatchingObjSizeException(String message) {
        super(message);
    }

}


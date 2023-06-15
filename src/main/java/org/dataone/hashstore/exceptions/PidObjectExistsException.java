package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore pidObjects
 */
public class PidObjectExistsException extends IOException {
    public PidObjectExistsException(String message) {
        super(message);
    }

    public PidObjectExistsException(String message, Throwable cause) {
        super(message, cause);
    }

    public PidObjectExistsException(Throwable cause) {
        super(cause);
    }
}

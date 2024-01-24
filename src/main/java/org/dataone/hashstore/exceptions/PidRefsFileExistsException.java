package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore pidObjects
 */
public class PidRefsFileExistsException extends IOException {
    public PidRefsFileExistsException(String message) {
        super(message);
    }

}

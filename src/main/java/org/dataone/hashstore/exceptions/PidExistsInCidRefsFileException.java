package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore pidObjects
 */
public class PidExistsInCidRefsFileException extends IOException {
    public PidExistsInCidRefsFileException(String message) {
        super(message);
    }

}

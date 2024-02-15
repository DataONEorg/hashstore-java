package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore when a pid is not found in a cid refs file.
 */
public class PidNotFoundInCidRefsFileException extends IOException {
    public PidNotFoundInCidRefsFileException(String message) {
        super(message);
    }

}

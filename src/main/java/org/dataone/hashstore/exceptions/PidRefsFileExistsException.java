package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class thrown when a pid refs file already exists (a single pid can only ever
 * reference one cid)
 */
public class PidRefsFileExistsException extends IOException {
    public PidRefsFileExistsException(String message) {
        super(message);
    }

}

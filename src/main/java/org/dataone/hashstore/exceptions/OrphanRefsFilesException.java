package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore when both a pid and cid reference file is found
 * but object does not exist.
 */
public class OrphanRefsFilesException extends IOException {
    public OrphanRefsFilesException(String message) {
        super(message);
    }

}

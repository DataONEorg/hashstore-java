package org.dataone.hashstore.exceptions;

/**
 * Custom exception class for FileHashStore when the expected cid is not found in the pid refs file.
 */
public class CidNotFoundInPidRefsFileException extends IllegalArgumentException {

    public CidNotFoundInPidRefsFileException(String message) {
        super(message);
    }

}

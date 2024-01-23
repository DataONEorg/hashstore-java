package org.dataone.hashstore.exceptions;

import java.io.IOException;

/**
 * Custom exception class for FileHashStore when a pid reference file is found and the
 * cid refs file that it is referencing does not contain the pid.
 */
public class OrphanPidRefsFileException extends IOException {
    public OrphanPidRefsFileException(String message) {
        super(message);
    }

}

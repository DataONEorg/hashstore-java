package org.dataone.hashstore.exceptions;

import java.nio.file.FileAlreadyExistsException;

/**
 * Custom exception thrown when called to tag a pid and cid, and reference files already exist
 */
public class HashStoreRefsAlreadyExistException extends FileAlreadyExistsException {

    public HashStoreRefsAlreadyExistException(String message) {
        super(message);
    }

}

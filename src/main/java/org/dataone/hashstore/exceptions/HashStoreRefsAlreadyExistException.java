package org.dataone.hashstore.exceptions;

import java.nio.file.FileAlreadyExistsException;

public class HashStoreRefsAlreadyExistException extends FileAlreadyExistsException {

    public HashStoreRefsAlreadyExistException(String message) {
        super(message);
    }

}

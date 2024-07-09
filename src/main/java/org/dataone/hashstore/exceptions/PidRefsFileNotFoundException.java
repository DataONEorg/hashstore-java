package org.dataone.hashstore.exceptions;

import java.io.FileNotFoundException;

public class PidRefsFileNotFoundException extends FileNotFoundException {
    public PidRefsFileNotFoundException(String message) {
        super(message);
    }

}

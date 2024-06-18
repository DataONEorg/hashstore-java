package org.dataone.hashstore.exceptions;

import java.io.IOException;

public class NonMatchingObjSizeException extends IllegalArgumentException {

    public NonMatchingObjSizeException(String message) {
        super(message);
    }

}


package org.dataone.hashstore.exceptions;

import java.io.IOException;

public class NonMatchingChecksumException extends IllegalArgumentException {

    public NonMatchingChecksumException(String message) {
        super(message);
    }

}

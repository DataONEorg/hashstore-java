package org.dataone.hashstore.exceptions;

import java.io.IOException;

public class NonMatchingChecksumException extends IOException {

    public NonMatchingChecksumException(String message) {
        super(message);
    }
}

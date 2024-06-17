package org.dataone.hashstore.exceptions;

public class UnsupportedHashAlgorithmException extends IllegalArgumentException {

    public UnsupportedHashAlgorithmException(String message) {
        super(message);
    }

}

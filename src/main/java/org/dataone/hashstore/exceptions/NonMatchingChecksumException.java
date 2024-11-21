package org.dataone.hashstore.exceptions;

import java.util.Map;

/**
 * An exception thrown when a checksum does not match what is expected.
 */

public class NonMatchingChecksumException extends IllegalArgumentException {

    private final Map<String, String> hexDigests;

    public NonMatchingChecksumException(String message, Map<String, String> checksumMap) {
        super(message);
        this.hexDigests = checksumMap;
    }

    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}

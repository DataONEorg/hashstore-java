package org.dataone.hashstore.exceptions;

import java.util.NoSuchElementException;

/**
 * An exception thrown when hexDigests from a supplied ObjectMetadata object is empty.
 */
public class MissingHexDigestsException extends NoSuchElementException {

        public MissingHexDigestsException(String message) {
            super(message);
        }

}

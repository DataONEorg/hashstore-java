package org.dataone.hashstore.filehashstore;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.xml.bind.DatatypeConverter;

/**
 * FileHashStoreUtility is a utility class that provides shared functionality between FileHashStore
 * and related classes.
 */
public class FileHashStoreUtility {

    /**
     * Checks whether a given object is null and throws an exception if so
     *
     * @param object   Object to check
     * @param argument Value that is being checked
     * @param method   Calling method or class
     */
    public static void ensureNotNull(Object object, String argument, String method) {
        if (object == null) {
            String errMsg = "FileHashStoreUtility.isStringNullOrEmpty - Calling Method: " + method
                + "(): " + argument + " cannot be null.";
            throw new NullPointerException(errMsg);
        }
    }

    /**
     * Calculate the hex digest of a pid's respective object with the given algorithm
     *
     * @param dataStream InputStream to object
     * @param algorithm  Hash algorithm to use
     * @return Hex digest of the pid's respective object
     * @throws IOException              Error when calculating hex digest
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    public static String calculateHexDigest(InputStream dataStream, String algorithm)
        throws IOException, NoSuchAlgorithmException {
        MessageDigest mdObject = MessageDigest.getInstance(algorithm);
        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                mdObject.update(buffer, 0, bytesRead);

            }
            // Close dataStream
            dataStream.close();

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStoreUtility.calculateHexDigest - Unexpected IOException encountered: "
                    + ioe.getMessage();
            throw new IOException(errMsg);

        }
        // mdObjectHexDigest
        return DatatypeConverter.printHexBinary(mdObject.digest()).toLowerCase();

    }
}

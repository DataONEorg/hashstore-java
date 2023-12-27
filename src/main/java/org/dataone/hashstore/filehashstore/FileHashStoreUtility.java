package org.dataone.hashstore.filehashstore;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

/**
 * FileHashStoreUtility is a utility class that encapsulates generic or shared functionality
 * in FileHashStore and/or related classes.
 */
public class FileHashStoreUtility {

    /**
     * Checks whether a given object is null and throws an exception if so
     *
     * @param object   Object to check
     * @param argument Value that is being checked
     * @param method   Calling method or class
     * @throws IllegalArgumentException If the object is null
     */
    public static void ensureNotNull(Object object, String argument, String method)
        throws IllegalArgumentException {
        if (object == null) {
            String errMsg = "FileHashStoreUtility.ensureNotNull - Calling Method: " + method
                + "(): " + argument + " cannot be null.";
            throw new IllegalArgumentException(errMsg);
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

        } catch (IOException ioe) {
            String errMsg =
                "FileHashStoreUtility.calculateHexDigest - Unexpected IOException encountered: "
                    + ioe.getMessage();
            throw new IOException(errMsg);

        } finally {
            // Close dataStream
            dataStream.close();
        }
        // mdObjectHexDigest
        return DatatypeConverter.printHexBinary(mdObject.digest()).toLowerCase();
    }

    /**
     * Checks whether a directory is empty or contains files. If a file is found, it returns true.
     *
     * @param directory Directory to check
     * @return True if a file is found or the directory is empty, False otherwise
     * @throws IOException If I/O occurs when accessing directory
     */
    public static boolean isDirectoryEmpty(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            // The findFirst() method is called on the stream created from the given
            // directory to retrieve the first element. If the stream is empty (i.e., the
            // directory is empty), findFirst() will return an empty Optional<Path>.
            //
            // The isPresent() method is called on the Optional<Path> returned by
            // findFirst(). If the Optional contains a value (i.e., an element was found),
            // isPresent() returns true. If the Optional is empty (i.e., the stream is
            // empty), isPresent() returns false.
            return !stream.findFirst().isPresent();
        }
    }

    /**
     * Checks whether a given string is empty and throws an exception if so
     *
     * @param string   String to check
     * @param argument Value that is being checked
     * @param method   Calling method
     * @throws IllegalArgumentException If the string is empty or null
     */
    public static void checkForEmptyString(String string, String argument, String method)
        throws IllegalArgumentException {
        ensureNotNull(string, "string", "checkForEmptyString");
        if (string.trim().isEmpty()) {
            String errMsg = "FileHashStoreUtility.checkForEmptyString - Calling Method: " + method
                + "(): " + argument + " cannot be empty.";
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Checks whether a given long integer is negative or zero
     *
     * @param longInt Object to check
     * @param method  Calling method
     * @throws IllegalArgumentException If longInt is less than or equal
     */
    public static void checkNotNegativeOrZero(long longInt, String method)
        throws IllegalArgumentException {
        if (longInt < 0 || longInt == 0) {
            String errMsg = "FileHashStoreUtility.checkNotNegative - Calling Method: " + method
                + "(): objSize cannot be less than or equal to 0.";
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Generates a hierarchical path by dividing a given digest into tokens of fixed width, and
     * concatenating them with '/' as the delimiter.
     *
     * @param depth  integer to represent number of directories
     * @param width  width of each directory
     * @param digest value to shard
     * @return String
     */
    public static String getHierarchicalPathString(int depth, int width, String digest) {
        List<String> tokens = new ArrayList<>();
        int digestLength = digest.length();
        for (int i = 0; i < depth; i++) {
            int start = i * width;
            int end = Math.min((i + 1) * width, digestLength);
            tokens.add(digest.substring(start, end));
        }

        if (depth * width < digestLength) {
            tokens.add(digest.substring(depth * width));
        }

        List<String> stringArray = new ArrayList<>();
        for (String str : tokens) {
            if (!str.trim().isEmpty()) {
                stringArray.add(str);
            }
        }
        // stringShard
        return String.join("/", stringArray);
    }

    /**
     * Creates an empty/temporary file in a given location. If this file is not moved, it will
     * be deleted upon JVM gracefully exiting or shutting down.
     *
     * @param prefix    string to prepend before tmp file
     * @param directory location to create tmp file
     * @return Temporary file ready to write into
     * @throws IOException       Issues with generating tmpFile
     * @throws SecurityException Insufficient permissions to create tmpFile
     */
    public static File generateTmpFile(String prefix, Path directory) throws IOException,
        SecurityException {
        Random rand = new Random();
        int randomNumber = rand.nextInt(1000000);
        String newPrefix = prefix + "-" + System.currentTimeMillis() + randomNumber;

        Path newPath = Files.createTempFile(directory, newPrefix, null);
        File newFile = newPath.toFile();
        newFile.deleteOnExit();
        return newFile;
    }
}

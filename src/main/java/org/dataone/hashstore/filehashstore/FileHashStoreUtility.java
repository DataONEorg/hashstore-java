package org.dataone.hashstore.filehashstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Stream;

import javax.xml.bind.DatatypeConverter;

/**
 * FileHashStoreUtility is a utility class that encapsulates generic or shared functionality
 * in FileHashStore and/or related classes.
 */
public class FileHashStoreUtility {

    private static final Log log = LogFactory.getLog(FileHashStoreUtility.class);

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
            String errMsg = "Calling Method: " + method + "(): " + argument + " cannot be null.";
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
        try (dataStream) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                mdObject.update(buffer, 0, bytesRead);

            }

        } catch (IOException ioe) {
            String errMsg = "Unexpected IOException encountered: " + ioe.getMessage();
            throw new IOException(errMsg);

        }
        // mdObjectHexDigest
        return DatatypeConverter.printHexBinary(mdObject.digest()).toLowerCase();
    }

    /**
     * Given a string and supported algorithm returns the hex digest
     *
     * @param pid       authority based identifier or persistent identifier
     * @param algorithm string value (ex. SHA-256)
     * @return Hex digest of the given string in lower-case
     * @throws IllegalArgumentException String or algorithm cannot be null or empty
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    public static String getPidHexDigest(String pid, String algorithm)
        throws NoSuchAlgorithmException, IllegalArgumentException {
        FileHashStoreUtility.ensureNotNull(pid, "pid", "getPidHexDigest");
        FileHashStoreUtility.checkForEmptyAndValidString(pid, "pid", "getPidHexDigest");
        FileHashStoreUtility.ensureNotNull(algorithm, "algorithm", "getPidHexDigest");
        FileHashStoreUtility.checkForEmptyAndValidString(algorithm, "algorithm", "getPidHexDigest");

        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = pid.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        // stringDigest
        return DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
    }

    /**
     * Checks whether a directory is empty or contains files. If a file is found, it returns true.
     *
     * @param directory Directory to check
     * @return True if a file is found or the directory is empty, False otherwise
     * @throws IOException If I/O occurs when accessing directory
     */
    public static boolean dirContainsFiles(Path directory) throws IOException {
        try (Stream<Path> stream = Files.list(directory)) {
            // The findFirst() method is called on the stream created from the given
            // directory to retrieve the first element. If the stream is empty (i.e., the
            // directory is empty), findFirst() will return an empty Optional<Path>.
            //
            // The isPresent() method is called on the Optional<Path> returned by
            // findFirst(). If the Optional contains a value (i.e., an element was found),
            // isPresent() returns true. If the Optional is empty (i.e., the stream is
            // empty), isPresent() returns false.
            return stream.findFirst().isPresent();
        }
    }

    /**
     * Checks a directory for files and returns a list of paths
     *
     * @param directory Directory to check
     * @return List<Path> of files
     * @throws IOException If I/O occurs when accessing directory
     */
    public static List<Path> getFilesFromDir(Path directory) throws IOException {
        List<Path> filePaths = new ArrayList<>();
        if (Files.isDirectory(directory) && dirContainsFiles(directory)) {
            try (Stream<Path> stream = Files.walk(directory)) {
                stream.filter(Files::isRegularFile).forEach(filePaths::add);
            }
        }
        return filePaths;
    }

    /**
     * Rename the given path to the 'file name' + '_delete'
     * 
     * @param pathToRename The path to the file to be renamed with '_delete'
     * @return Path to the file with '_delete' appended
     * @throws IOException Issue with renaming the given file path
     */
    public static Path renamePathForDeletion(Path pathToRename) throws IOException {
        ensureNotNull(pathToRename, "pathToRename", "renamePathForDeletion");
        if (!Files.exists(pathToRename)) {
            String errMsg = "Given path to file: " + pathToRename + " does not exist.";
            throw new FileNotFoundException(errMsg);
        }
        Path parentPath = pathToRename.getParent();
        Path fileName = pathToRename.getFileName();
        String newFileName = fileName.toString() + "_delete";

        Path deletePath = parentPath.resolve(newFileName);
        Files.move(pathToRename, deletePath, StandardCopyOption.ATOMIC_MOVE);
        return deletePath;
    }

    /**
     * Rename the given path slated for deletion by replacing '_delete' with ""
     *
     * @param pathToRename The path to the file to revert deletion
     * @throws IOException Issue with renaming the given file path
     */
    public static void renamePathForRestoration(Path pathToRename) throws IOException {
        ensureNotNull(pathToRename, "pathToRename", "renamePathForRestoration");
        if (!Files.exists(pathToRename)) {
            String errMsg = "Given path to file: " + pathToRename + " does not exist.";
            throw new FileNotFoundException(errMsg);
        }
        Path parentPath = pathToRename.getParent();
        Path fileName = pathToRename.getFileName();
        String newFileName = fileName.toString().replace("_delete", "");

        Path restorePath = parentPath.resolve(newFileName);
        Files.move(pathToRename, restorePath, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Delete all paths found in the given List<Path> object.
     *
     * @param deleteList Directory to check
     */
    public static void deleteListItems(Collection<Path> deleteList) {
        ensureNotNull(deleteList, "deleteList", "deleteListItems");
        if (!deleteList.isEmpty()) {
            for (Path deleteItem : deleteList) {
                if (Files.exists(deleteItem)) {
                    try {
                        Files.delete(deleteItem);
                    } catch (Exception ge) {
                        String warnMsg =
                            "Attempted to delete metadata document: " + deleteItem + " but failed."
                                + " Additional Details: " + ge.getMessage();
                        log.warn(warnMsg);
                    }

                }
            }
        }
    }

    /**
     * Checks whether a given string is empty or contains illegal characters, and throws an
     * exception if so
     *
     * @param string   String to check
     * @param argument Value that is being checked
     * @param method   Calling method
     * @throws IllegalArgumentException If the string is empty or contains illegal characters
     */
    public static void checkForEmptyAndValidString(String string, String argument, String method)
        throws IllegalArgumentException {
        ensureNotNull(string, "string", "checkForEmptyAndValidString");
        if (string.trim().isEmpty()) {
            String errMsg = "Calling Method: " + method + "(): " + argument + " cannot be empty.";
            throw new IllegalArgumentException(errMsg);
        }
        if (!isValidString(string)) {
            String errMsg = "Calling Method: " + method + "(): " + argument
                + " contains empty white spaces, tabs or newlines.";
            throw new IllegalArgumentException(errMsg);
        }
    }

    /**
     * Iterates over a given string and checks each character to make sure that there are no
     * whitespaces, tabs, new lines or other illegal characters.
     * @param string String to check
     * @return True if valid, False if illegal characters found.
     */
    public static boolean isValidString(String string) {
        boolean valid = true;
        for (int i = 0; i < string.length(); i++) {
            char ch = string.charAt(i);
            if (Character.isWhitespace(ch)) {
                valid = false;
                break;
            }
        }
        return valid;
    }

    /**
     * Checks whether a given long integer is negative or zero
     *
     * @param longInt Object to check
     * @param method  Calling method
     * @throws IllegalArgumentException If longInt is less than or equal
     */
    public static void checkPositive(long longInt, String method)
        throws IllegalArgumentException {
        if (longInt <= 0) {
            String errMsg =
                "Calling Method: " + method + "(): objSize cannot be less than or equal to 0.";
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
        Collection<String> tokens = new ArrayList<>();
        int digestLength = digest.length();
        for (int i = 0; i < depth; i++) {
            int start = i * width;
            int end = Math.min((i + 1) * width, digestLength);
            tokens.add(digest.substring(start, end));
        }

        if (depth * width < digestLength) {
            tokens.add(digest.substring(depth * width));
        }

        Collection<String> stringArray = new ArrayList<>();
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

    /**
     * Ensures that two objects are equal. If not, throws an IllegalArgumentException.
     *
     * @param nameValue  The name of the object being checked
     * @param suppliedValue The value supplied to compare
     * @param existingValue The existing value to compare with
     * @throws IllegalArgumentException If the supplied value is not equal to the existing value
     */
    public static void checkObjectEquality(
        String nameValue, Object suppliedValue, Object existingValue) {
        if (!Objects.equals(suppliedValue, existingValue)) {
            String errMsg =
                "FileHashStore.checkConfigurationEquality() - Mismatch in " + nameValue + ": "
                    + suppliedValue + " does not match the existing configuration value: "
                    + existingValue;
            throw new IllegalArgumentException(errMsg);
        }
    }
}

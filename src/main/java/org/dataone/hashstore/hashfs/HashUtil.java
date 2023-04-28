package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * HashUtil provides utility methods for HashFileStore
 */
public class HashUtil {
    public String[] supportedHashAlgorithms = { "MD2", "MD5", "SHA-1", "SHA-256", "SHA-384", "SHA-512", "SHA-512/224",
            "SHA-512/256" };

    /**
     * Checks whether a given algorithm is supported based on the HashUtil class
     * variable supportedHashAlgorithms
     * 
     * @param algorithm
     * @return boolean that describes whether an algorithm is supported
     * @throws NullPointerException
     */
    public boolean isValidAlgorithm(String algorithm) throws NullPointerException {
        if (algorithm == null) {
            throw new NullPointerException("algorithm supplied is null: " + algorithm);
        }
        if (!Arrays.asList(this.supportedHashAlgorithms).contains(algorithm) && algorithm != null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * Creates an empty file in a given location
     * 
     * @param prefix
     * @param directory
     * 
     * @return Temporary file (File) ready to write into
     * @throws IOException
     * @throws SecurityException
     */
    public File generateTmpFile(String prefix, File directory) throws IOException, SecurityException {
        String newPrefix = prefix + "-" + System.currentTimeMillis();
        String suffix = null;
        File newFile = null;
        try {
            newFile = File.createTempFile(newPrefix, suffix, directory);
        } catch (IOException ioe) {
            // TODO: Log Exception ioe
            throw new IOException("Unable to generate tmpFile. IOException: " + ioe.getMessage());
        } catch (SecurityException se) {
            // TODO: Log Exception se
            throw new SecurityException("File not allowed (security manager exists): " + se.getMessage());
        }
        // TODO: Log - newFile.getCanonicalPath());
        return newFile;
    }

    /**
     * Create a list of 'depth' number of tokens with 'width' with the last item
     * being the remainder of the digest, delimited by "/"
     * 
     * @param depth
     * @param width
     * @param digest
     * @return
     */
    public String shard(int depth, int width, String digest) {
        List<String> tokens = new ArrayList<String>();
        int digestLength = digest.length();
        for (int i = 0; i < depth; i++) {
            int start = i * width;
            int end = Math.min((i + 1) * width, digestLength);
            tokens.add(digest.substring(start, end));
        }
        if (depth * width < digestLength) {
            tokens.add(digest.substring(depth * width));
        }
        List<String> stringArray = new ArrayList<String>();
        for (String str : tokens) {
            if (!str.isEmpty()) {
                stringArray.add(str);
            }
        }
        String stringShard = "/" + String.join("/", stringArray);
        return stringShard;
    }

    /**
     * Given a string and supported algorithm returns the hex digest
     * 
     * @param string    authority based identifier or persistent identifier
     * @param algorithm
     * 
     * @return Hex digest of the given string in lower-case
     * @throws IllegalArgumentException
     * @throws NoSuchAlgorithmException
     */
    public String getHexDigest(String string, String algorithm) throws NoSuchAlgorithmException {
        boolean algorithmSupported = this.isValidAlgorithm(algorithm);
        if (algorithm == null || algorithm.isEmpty() | string == null || string.isEmpty()) {
            throw new IllegalArgumentException(
                    "Algorithm and/or string cannot be null or empty: ");
        }
        if (!algorithmSupported) {
            throw new NoSuchAlgorithmException(
                    "Algorithm not supported. Supported algorithms: " + Arrays.toString(supportedHashAlgorithms));
        }
        MessageDigest stringMessageDigest = MessageDigest.getInstance(algorithm);
        byte[] bytes = string.getBytes(StandardCharsets.UTF_8);
        stringMessageDigest.update(bytes);
        String stringDigest = DatatypeConverter.printHexBinary(stringMessageDigest.digest()).toLowerCase();
        return stringDigest;
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap
     * consisting of algorithms and their respective hex digests. If an additional
     * algorithm is supplied and supported, it and its checksum value will be
     * included in the hex digests map.
     * 
     * Default algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
     * 
     * @param tmpFile
     * @param dataStream
     * @param additionalAlgorithm
     * 
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException
     * @throws IOException
     * @throws SecurityException
     * @throws FileNotFoundException
     */
    protected Map<String, String> writeToTmpFileAndGenerateChecksums(File tmpFile, InputStream dataStream,
            String additionalAlgorithm)
            throws NoSuchAlgorithmException, IOException, FileNotFoundException, SecurityException {
        if (additionalAlgorithm != null) {
            if (additionalAlgorithm.isEmpty()) {
                throw new IllegalArgumentException(
                        "Additional algorithm cannot be empty");
            }
            boolean algorithmSupported = this.isValidAlgorithm(additionalAlgorithm);
            if (!algorithmSupported) {
                throw new IllegalArgumentException(
                        "Algorithm not supported. Supported algorithms: " + Arrays.toString(supportedHashAlgorithms));
            }
        }

        MessageDigest extraAlgo = null;
        Map<String, String> hexDigests = new HashMap<>();

        FileOutputStream os = new FileOutputStream(tmpFile);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        MessageDigest sha384 = MessageDigest.getInstance("SHA-384");
        MessageDigest sha512 = MessageDigest.getInstance("SHA-512");
        if (additionalAlgorithm != null) {
            extraAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }

        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                os.write(buffer, 0, bytesRead);
                md5.update(buffer, 0, bytesRead);
                sha1.update(buffer, 0, bytesRead);
                sha256.update(buffer, 0, bytesRead);
                sha384.update(buffer, 0, bytesRead);
                sha512.update(buffer, 0, bytesRead);
                if (additionalAlgorithm != null) {
                    extraAlgo.update(buffer, 0, bytesRead);
                }
            }
        } finally {
            try {
                os.flush();
                os.close();
            } catch (Exception e) {
                // TODO: Log exception
            }
        }

        String md5Digest = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put("MD5", md5Digest);
        hexDigests.put("SHA-1", sha1Digest);
        hexDigests.put("SHA-256", sha256Digest);
        hexDigests.put("SHA-384", sha384Digest);
        hexDigests.put("SHA-512", sha512Digest);
        if (additionalAlgorithm != null) {
            String extraDigest = DatatypeConverter.printHexBinary(extraAlgo.digest()).toLowerCase();
            hexDigests.put(additionalAlgorithm, extraDigest);
        }

        return hexDigests;
    }

    /**
     * Moves an object from one location to another if the object does not exist
     * 
     * @param source
     * @param target
     * 
     * @return boolean to confirm file is not a duplicate
     * @throws IOException
     */
    protected boolean move(File source, File target) throws IOException, SecurityException {
        boolean isDuplicate = false;
        if (target.exists()) {
            isDuplicate = true;
        } else {
            File destinationDirectory = new File(target.getParent());
            // Create parent directory if it doesn't exist
            if (!destinationDirectory.exists()) {
                Path destinationDirectoryPath = destinationDirectory.toPath();
                Files.createDirectories(destinationDirectoryPath);
            }

            // Move file
            Path sourceFilePath = source.toPath();
            Path targetFilePath = target.toPath();
            try {
                Files.move(sourceFilePath, targetFilePath, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException amnse) {
                // TODO: Log exception and specify atomic_move not possible
                Files.move(sourceFilePath, targetFilePath);
            } catch (IOException ioe) {
                // TODO: Log failure - include signature values, ioe
                throw ioe;
            }
        }
        return isDuplicate;
    }
}

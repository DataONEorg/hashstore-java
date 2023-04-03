package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * HashUtil provides utility methods for HashFileStore
 */
public class HashUtil {
    /**
     * Creates an empty file in a given location
     * 
     * @param prefix
     * @param directory
     * @return
     * @throws IOException
     */
    public File generateTmpFile(String prefix, File directory) throws IOException {
        String newPrefix = prefix + "-" + System.currentTimeMillis();
        String suffix = null;
        File newFile = null;
        try {
            newFile = File.createTempFile(newPrefix, suffix, directory);
        } catch (Exception e) {
            // try again if the first time fails
            newFile = File.createTempFile(newPrefix, suffix, directory);
            // TODO: Log Exception e
        }
        // TODO: Log - newFile.getCanonicalPath());
        return newFile;
    }

    /**
     * Write the input stream into a given file (tmpFile) and return a HashMap
     * consisting of algorithms and their respective hex digests
     * 
     * Default algorithms: MD5, SHA-1, SHA-256, SHA-384, SHA-512
     * 
     * @param tmpFile             File into which the stream will be written to
     * @param dataStream          Source data stream
     * @param additionalAlgorithm Optional additional algoritm to generate
     */
    public Map<String, String> writeToTmpFileAndGenerateChecksums(File tmpFile, InputStream dataStream,
            String additionalAlgorithm) throws NoSuchAlgorithmException, IOException {
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
            if (os != null) {
                try {
                    os.flush();
                    os.close();
                } catch (Exception e) {
                    // TODO: Log exception
                }
            }
        }

        String md5Digest = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put("MD-5", md5Digest);
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
     * Create a list of 'depth' number of tokens with 'width' with the last item
     * being the remainder of the digest, and return a String path
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
        String shardedPath = "/" + String.join("/", stringArray);
        return shardedPath;
    }

    /**
     * Moves an object from one directory to another if the object does not exist
     * 
     * @param source
     * @param target
     * @return
     * @throws IOException
     */
    public boolean move(File source, File target) throws IOException {
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
            } catch (IOException ioe) {
                // TODO: Log failure - include signature values, ioe
                throw ioe;
            }
        }
        return isDuplicate;
    }
}

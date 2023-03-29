package org.dataone.hashstore.hashfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * HashUtil provides utility methods for HashFileStore
 */
public class HashUtil {
    /**
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
     * Write the input stream into a given file (tmpFile) and return a HashMap of
     * the default algorithms and their respective hex digests
     * 
     * @param tmpFile             File into which the stream will be written to
     * @param dataStream          Source data stream
     * @param additionalAlgorithm Optional additional algoritm to generate
     */
    public Map<String, String> writeToTmpFileAndGenerateChecksums(File tmpFile, InputStream dataStream,
            String additionalAlgorithm) throws NoSuchAlgorithmException, IOException {
        // TODO: Handle additionalAlgorithm when not null
        Map<String, String> hexDigests = new HashMap<>();

        FileOutputStream os = new FileOutputStream(tmpFile);
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        MessageDigest sha256 = MessageDigest.getInstance("SHA-256");
        MessageDigest sha384 = MessageDigest.getInstance("SHA-384");
        MessageDigest sha512 = MessageDigest.getInstance("SHA-512");

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

        String md5Checksum = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put("md5", md5Checksum);
        hexDigests.put("sha1", sha1Digest);
        hexDigests.put("sha256", sha256Digest);
        hexDigests.put("sha384", sha384Digest);
        hexDigests.put("sha512", sha512Digest);

        return hexDigests;
    }

}

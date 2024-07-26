package org.dataone.hashstore.hashstoreconverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.ObjectMetadata;
import org.dataone.hashstore.exceptions.PidRefsFileExistsException;
import org.dataone.hashstore.filehashstore.FileHashStore;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class FileHashStoreLinks extends FileHashStore {

    private static final Log logFileHashStoreLinks = LogFactory.getLog(FileHashStore.class);

    public FileHashStoreLinks(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        super(hashstoreProperties);
        logFileHashStoreLinks.info("FileHashStoreLinks initialized");
    }

    /**
     * Get a HashMap consisting of algorithms and their respective hex digests for a given
     * data stream. If an additional algorithm is supplied and supported, it and its checksum
     * value will be included in the hex digests map. Default algorithms: MD5, SHA-1, SHA-256,
     * SHA-384, SHA-512
     *
     * @param dataStream          input stream of data to store
     * @param additionalAlgorithm additional algorithm to include in hex digest map
     * @param checksumAlgorithm   checksum algorithm to calculate hex digest for to verifying
     *                            object
     * @return A map containing the hex digests of the default algorithms
     * @throws NoSuchAlgorithmException Unable to generate new instance of supplied algorithm
     * @throws IOException              Issue with writing file from InputStream
     * @throws SecurityException        Unable to write to tmpFile
     * @throws FileNotFoundException    tmpFile cannot be found
     */
    protected Map<String, String> generateChecksums(InputStream dataStream,
                                                  String additionalAlgorithm, String checksumAlgorithm
    ) throws NoSuchAlgorithmException, IOException, SecurityException {
        // Determine whether to calculate additional or checksum algorithms
        boolean generateAddAlgo = false;
        if (additionalAlgorithm != null) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                additionalAlgorithm, "additionalAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(additionalAlgorithm);
            generateAddAlgo = shouldCalculateAlgorithm(additionalAlgorithm);
        }
        boolean generateCsAlgo = false;
        if (checksumAlgorithm != null && !checksumAlgorithm.equals(additionalAlgorithm)) {
            FileHashStoreUtility.checkForEmptyAndValidString(
                checksumAlgorithm, "checksumAlgorithm", "writeToTmpFileAndGenerateChecksums"
            );
            validateAlgorithm(checksumAlgorithm);
            generateCsAlgo = shouldCalculateAlgorithm(checksumAlgorithm);
        }

        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.getName());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.getName());
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.getName());
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.getName());
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.getName());
        MessageDigest additionalAlgo = null;
        MessageDigest checksumAlgo = null;
        if (generateAddAlgo) {
            logFileHashStoreLinks.debug(
                "Adding additional algorithm to hex digest map, algorithm: " + additionalAlgorithm);
            additionalAlgo = MessageDigest.getInstance(additionalAlgorithm);
        }
        if (generateCsAlgo) {
            logFileHashStoreLinks.debug(
                "Adding checksum algorithm to hex digest map, algorithm: " + checksumAlgorithm);
            checksumAlgo = MessageDigest.getInstance(checksumAlgorithm);
        }

        // Calculate hex digests
        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = dataStream.read(buffer)) != -1) {
                md5.update(buffer, 0, bytesRead);
                sha1.update(buffer, 0, bytesRead);
                sha256.update(buffer, 0, bytesRead);
                sha384.update(buffer, 0, bytesRead);
                sha512.update(buffer, 0, bytesRead);
                if (generateAddAlgo) {
                    additionalAlgo.update(buffer, 0, bytesRead);
                }
                if (generateCsAlgo) {
                    checksumAlgo.update(buffer, 0, bytesRead);
                }
            }

        } catch (IOException ioe) {
            String errMsg = "Unexpected Exception ~ " + ioe.getMessage();
            logFileHashStoreLinks.error(errMsg);
            throw ioe;

        } finally {
            dataStream.close();
        }

        // Create map of hash algorithms and corresponding hex digests
        Map<String, String> hexDigests = new HashMap<>();
        String md5Digest = DatatypeConverter.printHexBinary(md5.digest()).toLowerCase();
        String sha1Digest = DatatypeConverter.printHexBinary(sha1.digest()).toLowerCase();
        String sha256Digest = DatatypeConverter.printHexBinary(sha256.digest()).toLowerCase();
        String sha384Digest = DatatypeConverter.printHexBinary(sha384.digest()).toLowerCase();
        String sha512Digest = DatatypeConverter.printHexBinary(sha512.digest()).toLowerCase();
        hexDigests.put(DefaultHashAlgorithms.MD5.getName(), md5Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_1.getName(), sha1Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_256.getName(), sha256Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_384.getName(), sha384Digest);
        hexDigests.put(DefaultHashAlgorithms.SHA_512.getName(), sha512Digest);
        if (generateAddAlgo) {
            String extraAlgoDigest = DatatypeConverter.printHexBinary(additionalAlgo.digest())
                .toLowerCase();
            hexDigests.put(additionalAlgorithm, extraAlgoDigest);
        }
        if (generateCsAlgo) {
            String extraChecksumDigest = DatatypeConverter.printHexBinary(checksumAlgo.digest())
                .toLowerCase();
            hexDigests.put(checksumAlgorithm, extraChecksumDigest);
        }

        return hexDigests;
    }

}

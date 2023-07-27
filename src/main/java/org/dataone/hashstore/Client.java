package org.dataone.hashstore;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

public class Client {
    private static HashStore hashStore;

    enum DefaultHashAlgorithms {
        MD5("MD5"), SHA_1("SHA-1"), SHA_256("SHA-256"), SHA_384("SHA-384"), SHA_512("SHA-512");

        final String algoName;

        DefaultHashAlgorithms(String algo) {
            algoName = algo;
        }

        public String getName() {
            return algoName;
        }
    }

    public static void main(String[] args) throws Exception {

        try {
            Path storePath = Paths.get("/home/mok/testing/knbvm_hashstore");

            // Initialize HashStore
            String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";

            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", storePath.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty(
                "storeMetadataNamespace", "http://ns.dataone.org/service/types/v2.0"
            );

            // Get HashStore
            hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);

            // Get guid, checksum and algorithm from metacat db into an array
            // TODO: Loop over array with the following pattern

            String pid = "test";
            // TODO: Ensure algorithm is formatted properly
            String algorithm = "SHA-256";
            String checksum = "abcdef12456789";

            // Retrieve object 
            InputStream pidObjStream = hashStore.retrieveObject(pid);

            // Get hex digest
            String streamDigest = calculateHexDigest(pidObjStream, algorithm);

            // If checksums don't match, write a .txt file
            if (!streamDigest.equals(checksum)) {
                // Create directory to store the error files
                Path errorDirectory = Paths.get(
                    "/home/mok/testing/knbvm_hashstore/java/obj/errors"
                );
                Files.createDirectories(errorDirectory);
                Path objectErrorTxtFile = errorDirectory.resolve("/" + pid + ".txt");

                String errMsg = "Obj retrieved (pid/guid): " + pid
                    + ". Checksums do not match, checksum from db: " + checksum
                    + ". Calculated digest: " + streamDigest + ". Algorithm: " + algorithm;

                try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(
                        Files.newOutputStream(objectErrorTxtFile), StandardCharsets.UTF_8
                    )
                )) {
                    writer.write(errMsg);

                } catch (Exception e) {
                    e.fillInStackTrace();
                }
            }

        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }

    /**
     * Calculate the hex digest of a pid's respective object with the given algorithm
     *
     * @param inputstream Path to object
     * @param algorithm   Hash algorithm to use
     * @return Hex digest of the pid's respective object
     * @throws IOException              Error when calculating hex digest
     * @throws NoSuchAlgorithmException Algorithm not supported
     */
    private static String calculateHexDigest(InputStream stream, String algorithm)
        throws IOException, NoSuchAlgorithmException {
        MessageDigest mdObject = MessageDigest.getInstance(algorithm);
        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = stream.read(buffer)) != -1) {
                mdObject.update(buffer, 0, bytesRead);

            }
            // Close stream
            stream.close();

        } catch (IOException ioe) {
            ioe.fillInStackTrace();

        }
        // mdObjectHexDigest
        return DatatypeConverter.printHexBinary(mdObject.digest()).toLowerCase();

    }
}

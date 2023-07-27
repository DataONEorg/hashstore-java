package org.dataone.hashstore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Properties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import javax.xml.bind.DatatypeConverter;

import org.dataone.hashstore.exceptions.HashStoreFactoryException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

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
        // Get a HashStore
        Path storePath = Paths.get("/home/mok/testing/knbvm_hashstore");
        initializeHashStore(storePath);

        // Load metacat db yaml
        Path pgdbYaml = storePath.resolve("pgdb.yaml");
        File pgdbYamlFile = pgdbYaml.toFile();
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        HashMap<?, ?> pgdbYamlProperties = om.readValue(pgdbYamlFile, HashMap.class);
        // Get db values
        String url = (String) pgdbYamlProperties.get("db_uri");
        String user = (String) pgdbYamlProperties.get("db_user");
        String password = (String) pgdbYamlProperties.get("db_password");

        try {
            // Setup metacat db access
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            String sqlQuery = "SELECT identifier.guid, identifier.docid, identifier.rev,"
                + " systemmetadata.object_format, systemmetadata.checksum,"
                + " systemmetadata.checksum_algorithm FROM identifier INNER JOIN systemmetadata"
                + " ON identifier.guid = systemmetadata.guid LIMIT 1000";
            ResultSet resultSet = statement.executeQuery(sqlQuery);

            // For each row, get guid, docid, rev, checksum and checksum_algorithm
            while (resultSet.next()) {
                String guid = resultSet.getString("guid");
                String docid = resultSet.getString("docid");
                int rev = resultSet.getInt("rev");
                String name = resultSet.getString("name");

                Path objfilePath = Paths.get("/var/metacat/data").resolve(docid + "." + rev);
                if (Files.exists(objfilePath)) {
                    // TODO: ...
                }
            }

            // Close resources
            resultSet.close();
            statement.close();
            connection.close();

            // TODO: Loop over final array generated with the pattern below
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
                Path objectErrorTxtFile = errorDirectory.resolve(pid + ".txt");

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

    private static void initializeHashStore(Path storePath) throws HashStoreFactoryException,
        IOException {
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

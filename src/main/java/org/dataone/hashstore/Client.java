package org.dataone.hashstore;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private static Path storePath = Paths.get("/home/mok/testing/knbvm_testlog");

    public static void main(String[] args) throws Exception {
        // Get a HashStore
        initializeHashStore(storePath);

        // Load metacat db yaml
        System.out.println("Loading metacat db yaml.");
        Path pgdbYaml = storePath.resolve("pgdb.yaml");
        File pgdbYamlFile = pgdbYaml.toFile();
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        HashMap<?, ?> pgdbYamlProperties = om.readValue(pgdbYamlFile, HashMap.class);
        // Get db values
        String url = (String) pgdbYamlProperties.get("db_uri");
        String user = (String) pgdbYamlProperties.get("db_user");
        String password = (String) pgdbYamlProperties.get("db_password");

        try {
            System.out.println("Connecting to metacat db.");
            // Setup metacat db access
            Class.forName("org.postgresql.Driver"); // Force driver to register itself
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            String sqlQuery = "SELECT identifier.guid, identifier.docid, identifier.rev,"
                + " systemmetadata.object_format, systemmetadata.checksum,"
                + " systemmetadata.checksum_algorithm FROM identifier INNER JOIN systemmetadata"
                + " ON identifier.guid = systemmetadata.guid LIMIT 100000;";
            ResultSet resultSet = statement.executeQuery(sqlQuery);

            // For each row, get guid, docid, rev, checksum and checksum_algorithm
            // and create a List to loop over
            List<Map<String, String>> resultObjList = new ArrayList<>();
            while (resultSet.next()) {
                System.out.println("Calling resultSet.next()");
                String guid = resultSet.getString("guid");
                String docid = resultSet.getString("docid");
                int rev = resultSet.getInt("rev");
                String checksum = resultSet.getString("checksum");
                String checksumAlgorithm = resultSet.getString("checksum_algorithm");
                String formattedAlgo = formatAlgo(checksumAlgorithm);

                Path setItemFilePath = Paths.get("/var/metacat/data/" + docid + "." + rev);
                if (Files.exists(setItemFilePath)) {
                    System.out.println("File exists: " + setItemFilePath);
                    Map<String, String> resultObj = new HashMap<>();
                    resultObj.put("pid", guid);
                    resultObj.put("algorithm", formattedAlgo);
                    resultObj.put("checksum", checksum);
                    resultObj.put("path", setItemFilePath.toString());

                    resultObjList.add(resultObj);
                }
            }

            // retrieveAndValidateObjs(resultObjList);
            storeObjectsWithChecksum(resultObjList);

            // Close resources
            resultSet.close();
            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static void storeObjectsWithChecksum(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                InputStream objStream = Files.newInputStream(Paths.get(item.get("path")));
                String algorithm = item.get("algorithm");
                String checksum = item.get("checksum");

                // Store object
                System.out.println("Storing object for guid: " + guid);
                hashStore.storeObject(objStream, guid, checksum, algorithm);

            } catch (IllegalArgumentException iae) {
                String errMsg = "Unexpected Error: " + iae.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_errors/illegalargument");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_errors/io");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            } catch (Exception e) {
                String errMsg = "Unexpected Error: " + e.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_errors/general");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            }
        });
    }

    private static void retrieveAndValidateObjs(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                String algorithm = item.get("algorithm");
                String checksum = item.get("checksum");

                // Retrieve object
                System.out.println("Retrieving object for guid: " + guid);
                InputStream objstream = hashStore.retrieveObject(guid);

                // Get hex digest
                System.out.println("Calculating hex digest with algorithm: " + algorithm);
                String streamDigest = calculateHexDigest(objstream, algorithm);

                // If checksums don't match, write a .txt file
                if (!streamDigest.equals(checksum)) {
                    String errMsg = "Obj retrieved (pid/guid): " + guid
                        + ". Checksums do not match, checksum from db: " + checksum
                        + ". Calculated digest: " + streamDigest + ". Algorithm: " + algorithm;
                    logExceptionToFile(guid, errMsg, "java/retrieve_errors/checksum_mismatch");
                } else {
                    System.out.println("Checksums match!");
                }

            } catch (FileNotFoundException fnfe) {
                String errMsg = "File not found: " + fnfe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_errors/filenotfound");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception e) {
                String errMsg = "Unexpected Error: " + e.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_errors/general");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            }
        });
    }

    private static void logExceptionToFile(String guid, String errMsg, String directory)
        throws Exception {
        // Create directory to store the error files
        Path errorDirectory = storePath.resolve(directory);
        Files.createDirectories(errorDirectory);
        Path objectErrorTxtFile = errorDirectory.resolve(guid + ".txt");

        try (BufferedWriter writer = new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(objectErrorTxtFile), StandardCharsets.UTF_8
            )
        )) {
            writer.write(errMsg);

        } catch (Exception e) {
            e.printStackTrace();

        }
    }

    /**
     * Format an algorithm string value to be compatible with MessageDigest class
     * 
     * @param value Algorithm value to format
     * @return Formatted algorithm value
     */
    private static String formatAlgo(String value) {
        // Temporary solution to format algorithm values
        // Query: SELECT DISTINCT checksum_algorithm FROM systemmetadata;
        // Output: MD5, SHA-256, SHA256, SHA-1, SHA1
        String upperValue = value.toUpperCase();
        String checkedAlgorithm = upperValue;
        if (upperValue.equals("SHA1")) {
            checkedAlgorithm = "SHA-1";
        }
        if (upperValue.equals("SHA256")) {
            checkedAlgorithm = "SHA-256";
        }
        return checkedAlgorithm;
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
     * @param stream    Path to object
     * @param algorithm Hash algorithm to use
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
            ioe.printStackTrace();

        }
        // mdObjectHexDigest
        return DatatypeConverter.printHexBinary(mdObject.digest()).toLowerCase();

    }
}

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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.dataone.hashstore.exceptions.HashStoreFactoryException;
import org.dataone.hashstore.exceptions.PidObjectExistsException;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Client {
    private static HashStore hashStore;
    private static Path storePath = Paths.get("/home/mok/testing/knbvm_hashstore");

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("No arguments provided. Use flag '-h' for help.");
        }
        // Add HashStore client options
        Options options = addHashStoreOptions();

        // Begin parsing options
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                formatter.printHelp("CommandLineApp", options);
            } else {
                // Get store path and get HashStore
                if (!cmd.hasOption("store")) {
                    String err_msg =
                        "HashStore store path must be supplied, use '-store=[path/to/store]'";
                    throw new IllegalArgumentException(err_msg);
                }
                Path storePath = Paths.get(cmd.getOptionValue("store"));
                // Confirm HashStore
                initializeHashStore(storePath);

                // Parse options
                if (cmd.hasOption("knbvm")) {
                    System.out.println(
                        "Testing with KNBVM values. Please ensure all config files present."
                    );
                    // TODO: Pass to method based on getOptions
                    String action = "sts";
                    String objType = "data"; // Or "documents"
                    testWithKnbvm(action, objType);
                }
            }
        } catch (ParseException e) {
            System.err.println("Error parsing cli arguments: " + e.getMessage());
            formatter.printHelp("CommandLineApp", options);
        }
    }

    private static void testWithKnbvm(String actionFlag, String objType) throws IOException,
        StreamReadException, DatabindException {
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
                + " ON identifier.guid = systemmetadata.guid ORDER BY identifier.guid;";
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
                String formattedChecksumAlgo = formatAlgo(checksumAlgorithm);
                String formatId = resultSet.getString("object_format");

                Path setItemFilePath = Paths.get(
                    "/var/metacat/" + objType + "/" + docid + "." + rev
                );

                if (Files.exists(setItemFilePath)) {
                    Map<String, String> resultObj = new HashMap<>();
                    if (objType == "data") {
                        resultObj.put("pid", guid);
                        resultObj.put("algorithm", formattedChecksumAlgo);
                        resultObj.put("checksum", checksum);
                        resultObj.put("path", setItemFilePath.toString());
                    }
                    if (objType == "documents") {
                        resultObj.put("pid", guid);
                        resultObj.put("path", setItemFilePath.toString());
                        resultObj.put("namespace", formatId);
                    }
                    resultObjList.add(resultObj);
                }
            }

            // Check option
            if (actionFlag == "sts") {
                // TODO: Refactor/update methods to be object/metadata specific
                // retrieveAndValidateObjs(resultObjList);
                // storeObjectsWithChecksum(resultObjList);
                // deleteObjectsFromStore(resultObjList);
                // storeMetadataFromDb(resultObjList);
                System.out.println("Placeholder");
            }

            // Close resources
            resultSet.close();
            statement.close();
            connection.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Create an options object to use with Apache Commons CLI library to manage command line
     * options.
     */
    private static Options addHashStoreOptions() {
        Options options = new Options();
        options.addOption("h", "help", false, "Show help options.");
        // Mandatory option
        options.addOption("store", "storepath", true, "Path to HashStore.");
        // HashStore creation options
        options.addOption("chs", "createhashstore", false, "Create a HashStore.");
        options.addOption("dp", "storedepth", true, "Depth of HashStore.");
        options.addOption("wp", "storewidth", true, "Width of HashStore.");
        options.addOption("ap", "storealgo", true, "Algorithm of HashStore.");
        options.addOption("nsp", "storenamespace", true, "Default metadata namespace");
        // Public API options
        options.addOption(
            "getchecksum", "client_getchecksum", false,
            "Get the hex digest of a data object in a HashStore"
        );
        options.addOption(
            "storeobject", "client_storeobject", false, "Store object to a HashStore."
        );
        options.addOption(
            "storemetadata", "client_storemetadata", false, "Store metadata to a HashStore"
        );
        options.addOption(
            "retrieveobject", "client_retrieveobject", false, "Retrieve an object from a HashStore."
        );
        options.addOption(
            "retrievemetadata", "client_retrievemetadata", false,
            "Retrieve a metadata obj from a HashStore."
        );
        options.addOption(
            "deleteobject", "client_deleteobject", false, "Delete an object from a HashStore."
        );
        options.addOption(
            "deletemetadata", "client_deletemetadata", false,
            "Delete a metadata obj from a HashStore."
        );
        options.addOption("pid", "pidguid", true, "PID or GUID of object.");
        options.addOption("path", "filepath", true, "Path to object.");
        options.addOption("algo", "objectalgo", true, "Algorithm to use in calculations.");
        options.addOption("checksum", "obj_checksum", true, "Checksum of object.");
        options.addOption(
            "checksum_algo", "obj_checksum_algo", true, "Algorithm of checksum supplied."
        );
        options.addOption("size", "obj_size", true, "Size of object");
        options.addOption("format_id", "metadata_format", true, "Metadata format_id/namespace");
        // knbvm (test.arcticdata.io) options
        options.addOption("knbvm", "knbvmtestadc", false, "Specify testing with knbvm.");
        options.addOption("nobj", "numberofobj", false, "Number of objects to work with.");
        options.addOption("sdir", "storedirectory", true, "Location of objects to convert.");
        options.addOption("stype", "storetype", true, "Type of store 'objects' or 'metadata'");
        options.addOption("sts", "storetohs", false, "Flag to store objs to a HashStore");
        options.addOption(
            "rav", "retandval", false, "Retrieve and validate objs from a HashStore."
        );
        options.addOption("dfs", "delfromhs", false, "Delete objs from a HashStore.");
        return options;
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

            } catch (PidObjectExistsException poee) {
                System.out.println("Object already exists for pid: " + guid);
                // String errMsg = "Unexpected Error: " + poee.fillInStackTrace();
                // try {
                //     logExceptionToFile(guid, errMsg, "java/store_errors/pidobjectexists");
                // } catch (Exception e1) {
                //     e1.printStackTrace();
                // }

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

    private static void deleteObjectsFromStore(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");

                // Delete object
                System.out.println("Deleting object for guid: " + guid);
                hashStore.deleteObject(guid);

            } catch (FileNotFoundException fnfe) {
                String errMsg = "Unexpected Error: " + fnfe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_errors/filenotfound");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_errors/io");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            } catch (Exception e) {
                String errMsg = "Unexpected Error: " + e.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_errors/general");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            }
        });
    }

    private static void storeMetadataFromDb(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                InputStream objStream = Files.newInputStream(Paths.get(item.get("path")));
                String formatId = item.get("namespace");

                // Store object
                System.out.println("Storing metadata for guid: " + guid);
                hashStore.storeMetadata(objStream, guid, formatId);

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_metadata_errors/io");
                } catch (Exception e1) {
                    e1.printStackTrace();
                }

            } catch (Exception e) {
                String errMsg = "Unexpected Error: " + e.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_metadata_errors/general");
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

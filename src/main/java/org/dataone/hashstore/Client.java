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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class Client {
    private static HashStore hashStore;
    private static Path storePath;

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            System.out.println("HashStoreClient - No arguments provided. Use flag '-h' for help.");
        }
        // Add HashStore client options
        Options options = addHashStoreClientOptions();

        // Begin parsing arguments
        CommandLineParser parser = new DefaultParser(false);
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);

            // First check if user is looking for help
            if (cmd.hasOption("h")) {
                formatter.printHelp("CommandLineApp", options);
                return;
            }

            // Then get store path and initialize HashStore
            if (!cmd.hasOption("store")) {
                String errMsg =
                    "HashStoreClient - store path must be supplied, use 'store=[path/to/store]'";
                throw new IllegalArgumentException(errMsg);
            }
            // Create or initialize HashStore
            if (cmd.hasOption("chs")) {
                String storePath = cmd.getOptionValue("store");
                String storeDepth = cmd.getOptionValue("dp");
                String storeWidth = cmd.getOptionValue("wp");
                String storeAlgorithm = cmd.getOptionValue("ap");
                String storeNameSpace = cmd.getOptionValue("nsp");
                createNewHashStore(
                    storePath, storeDepth, storeWidth, storeAlgorithm, storeNameSpace
                );
            } else {
                storePath = Paths.get(cmd.getOptionValue("store"));
                Path hashstoreYaml = storePath.resolve("hashstore.yaml");
                if (!Files.exists(hashstoreYaml)) {
                    String errMsg = "HashStoreClient - Missing hashstore.yaml at storePath ("
                        + storePath
                        + "), please create a store with '-chs'. Use '-h' to see options.";
                    throw new FileNotFoundException(errMsg);
                }
                initializeHashStore(storePath);

                // Parse remaining options
                if (cmd.hasOption("knbvm")) {
                    System.out.println(
                        "HashStoreClient - Testing with KNBVM, checking pgdb.yaml & hashstore.yaml."
                    );
                    Path pgdbYaml = storePath.resolve("pgdb.yaml");
                    if (!Files.exists(pgdbYaml)) {
                        String errMsg = "HashStoreClient - Missing pgdb.yaml at storePath ("
                            + storePath + "), please manually create it with the following keys: "
                            + "db_user, db_password, db_host, db_port, db_name";
                        throw new FileNotFoundException(errMsg);
                    }

                    String action = null;
                    if (cmd.hasOption("sts")) {
                        action = "sts";
                    }
                    if (cmd.hasOption("rav")) {
                        action = "rav";
                    }
                    if (cmd.hasOption("dfs")) {
                        action = "dfs";
                    }

                    String objType = cmd.getOptionValue("stype");
                    String originDirectory = cmd.getOptionValue("sdir");
                    String numObjects = cmd.getOptionValue("nobj");
                    ensureNotNull(objType, "-stype");
                    ensureNotNull(originDirectory, "-sdir");
                    ensureNotNull(action, "-sts, -rav, -dfs");
                    testWithKnbvm(action, objType, originDirectory, numObjects);

                } else if (cmd.hasOption("getchecksum")) {
                    String pid = cmd.getOptionValue("pid");
                    String algo = cmd.getOptionValue("algo");
                    ensureNotNull(pid, "-pid");
                    ensureNotNull(algo, "-algo");
                    String hexDigest = hashStore.getHexDigest(pid, algo);
                    System.out.println(hexDigest);

                } else if (cmd.hasOption("storeobject")) {
                    System.out.println("Storing object");
                    String pid = cmd.getOptionValue("pid");
                    Path path = Paths.get(cmd.getOptionValue("path"));
                    ensureNotNull(pid, "-pid");
                    ensureNotNull(path, "-path");
                    String additional_algo = null;
                    if (cmd.hasOption("algo")) {
                        additional_algo = cmd.getOptionValue("algo");
                    }
                    String checksum = null;
                    if (cmd.hasOption("checksum")) {
                        checksum = cmd.getOptionValue("checksum");
                    }
                    String checksum_algo = null;
                    if (cmd.hasOption("checksum_algo")) {
                        checksum_algo = cmd.getOptionValue("checksum_algo");
                    }
                    long size = 0;
                    if (cmd.hasOption("size")) {
                        size = Long.parseLong(cmd.getOptionValue("size"));
                    }

                    InputStream pidObjStream = Files.newInputStream(path);
                    ObjectInfo objInfo = hashStore.storeObject(
                        pidObjStream, pid, additional_algo, checksum, checksum_algo, size
                    );
                    pidObjStream.close();
                    System.out.println("Object Info for pid (" + pid + "):");
                    System.out.println(objInfo.getHexDigests());

                } else if (cmd.hasOption("storemetadata")) {
                    String pid = cmd.getOptionValue("pid");
                    Path path = Paths.get(cmd.getOptionValue("path"));
                    String formatId = cmd.getOptionValue("format_id");
                    ensureNotNull(pid, "-pid");
                    ensureNotNull(path, "-path");
                    ensureNotNull(formatId, "-format_id");

                    InputStream pidObjStream = Files.newInputStream(path);
                    String metadataCid = hashStore.storeMetadata(pidObjStream, pid, formatId);
                    pidObjStream.close();
                    System.out.println("Metadata Content Identifier:");
                    System.out.println(metadataCid);

                } else if (cmd.hasOption("retrieveobject")) {
                    String pid = cmd.getOptionValue("pid");
                    ensureNotNull(pid, "-pid");

                    InputStream objStream = hashStore.retrieveObject(pid);
                    byte[] buffer = new byte[1000];
                    int bytesRead = objStream.read(buffer, 0, buffer.length);
                    String objPreview = new String(buffer, 0, bytesRead, StandardCharsets.UTF_8);
                    objStream.close();
                    System.out.println(objPreview);

                } else if (cmd.hasOption("retrievemetadata")) {
                    String pid = cmd.getOptionValue("pid");
                    String formatId = cmd.getOptionValue("format_id");
                    ensureNotNull(pid, "-pid");
                    ensureNotNull(formatId, "-format_id");

                    InputStream metadataStream = hashStore.retrieveMetadata(pid, formatId);
                    byte[] buffer = new byte[1000];
                    int bytesRead = metadataStream.read(buffer, 0, buffer.length);
                    String metadataPreview = new String(
                        buffer, 0, bytesRead, StandardCharsets.UTF_8
                    );
                    metadataStream.close();
                    System.out.println(metadataPreview);

                } else if (cmd.hasOption("deleteobject")) {
                    String pid = cmd.getOptionValue("pid");
                    ensureNotNull(pid, "-pid");

                    hashStore.deleteObject(pid);
                    System.out.println("Object for pid (" + pid + ") has been deleted.");

                } else if (cmd.hasOption("deletemetadata")) {
                    String pid = cmd.getOptionValue("pid");
                    String formatId = cmd.getOptionValue("format_id");
                    ensureNotNull(pid, "-pid");
                    ensureNotNull(formatId, "-format_id");

                    hashStore.deleteMetadata(pid, formatId);
                    System.out.println(
                        "Metadata for pid (" + pid + ") and namespace (" + formatId
                            + ") has been deleted."
                    );
                } else {
                    System.out.println("HashStoreClient - No options found, use -h for help.");
                }
            }

        } catch (ParseException e) {
            System.err.println("Error parsing cli arguments: " + e.getMessage());
            formatter.printHelp("HashStore Client Options", options);
        }
    }


    // Configuration methods to initialize HashStore client

    /**
     * Returns an options object to use with Apache Commons CLI library to manage command line
     * options for HashStore client.
     */
    private static Options addHashStoreClientOptions() {
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

    /**
     * Create a new HashStore with the given properties.
     * 
     * @param storePath      Path to HashStore.
     * @param storeDepth     Depth of store.
     * @param storeWidth     Width of store.
     * @param storeAlgorithm Algorithm to use.
     * @param storeNameSpace Default metadata namespace.
     * @throws HashStoreFactoryException When unable to get HashStore from factory.
     */
    private static void createNewHashStore(
        String storePath, String storeDepth, String storeWidth, String storeAlgorithm,
        String storeNameSpace
    ) throws HashStoreFactoryException, IOException {
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", storePath);
        storeProperties.setProperty("storeDepth", storeDepth);
        storeProperties.setProperty("storeWidth", storeWidth);
        storeProperties.setProperty("storeAlgorithm", storeAlgorithm);
        storeProperties.setProperty("storeMetadataNamespace", storeNameSpace);

        // Get HashStore
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);
    }

    /**
     * Get the properties of HashStore from 'hashstore.yaml'
     *
     * @param storePath Path to root of store
     * @return HashMap of the properties
     */
    private static HashMap<String, Object> loadHashStoreYaml(Path storePath) {
        Path hashStoreYamlPath = storePath.resolve("hashstore.yaml");
        File hashStoreYamlFile = hashStoreYamlPath.toFile();
        ObjectMapper om = new ObjectMapper(new YAMLFactory());
        HashMap<String, Object> hsProperties = new HashMap<>();

        try {
            HashMap<?, ?> hashStoreYamlProperties = om.readValue(hashStoreYamlFile, HashMap.class);
            String yamlStorePath = (String) hashStoreYamlProperties.get("store_path");
            hsProperties.put("storePath", Paths.get(yamlStorePath));
            hsProperties.put("storeDepth", hashStoreYamlProperties.get("store_depth"));
            hsProperties.put("storeWidth", hashStoreYamlProperties.get("store_width"));
            hsProperties.put("storeAlgorithm", hashStoreYamlProperties.get("store_algorithm"));
            hsProperties.put(
                "storeMetadataNamespace", hashStoreYamlProperties.get("store_metadata_namespace")
            );

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }

        return hsProperties;
    }

    /**
     * Initialize HashStore to use in client app. HashStore must already exist or an exception will
     * be thrown.
     * 
     * @param storePath Path to store.
     * @throws HashStoreFactoryException If unable to initialize HashStore.
     * @throws IOException               If 'hashstore.yaml' cannot be loaded.
     * @throws FileNotFoundException     When 'hashstore.yaml' is missing.
     */
    private static void initializeHashStore(Path storePath) throws HashStoreFactoryException,
        IOException {
        // Load properties and get HashStore
        HashMap<String, Object> hsProperties = loadHashStoreYaml(storePath);
        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", hsProperties.get("storePath").toString());
        storeProperties.setProperty("storeDepth", hsProperties.get("storeDepth").toString());
        storeProperties.setProperty("storeWidth", hsProperties.get("storeWidth").toString());
        storeProperties.setProperty(
            "storeAlgorithm", hsProperties.get("storeAlgorithm").toString()
        );
        storeProperties.setProperty(
            "storeMetadataNamespace", hsProperties.get("storeMetadataNamespace").toString()
        );

        // Get HashStore
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);
    }


    // Core methods for testing in Knbvm (test.arcticdata.io)

    /**
     * Entry point for working with test data found in knbvm (test.arcticdata.io)
     * 
     * @param actionFlag String representing a knbvm test-related method to call.
     * @param objType    "data" (objects) or "documents" (metadata).
     * @param numObjects Number of rows to retrieve from metacat db,
     *                   if null, will retrieve all rows.
     * @throws IOException Related to accessing config files or objects
     */
    private static void testWithKnbvm(
        String actionFlag, String objType, String originDir, String numObjects
    ) throws IOException {
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

        String sqlLimitQuery = "";
        if (numObjects != null) {
            sqlLimitQuery = " LIMIT " + numObjects;
        }

        try {
            System.out.println("Connecting to metacat db.");
            // Setup metacat db access
            Class.forName("org.postgresql.Driver"); // Force driver to register itself
            Connection connection = DriverManager.getConnection(url, user, password);
            Statement statement = connection.createStatement();
            String sqlQuery = "SELECT identifier.guid, identifier.docid, identifier.rev,"
                + " systemmetadata.object_format, systemmetadata.checksum,"
                + " systemmetadata.checksum_algorithm FROM identifier INNER JOIN systemmetadata"
                + " ON identifier.guid = systemmetadata.guid ORDER BY identifier.guid"
                + sqlLimitQuery + ";";
            ResultSet resultSet = statement.executeQuery(sqlQuery);

            // For each row, get guid, docid, rev, checksum and checksum_algorithm
            // and create a List to loop over
            List<Map<String, String>> resultObjList = new ArrayList<>();
            while (resultSet.next()) {
                String guid = resultSet.getString("guid");
                String docid = resultSet.getString("docid");
                int rev = resultSet.getInt("rev");
                String checksum = resultSet.getString("checksum");
                String checksumAlgorithm = resultSet.getString("checksum_algorithm");
                String formattedChecksumAlgo = formatAlgo(checksumAlgorithm);
                String formatId = resultSet.getString("object_format");

                if (!objType.equals("object")) {
                    if (!objType.equals("metadata")) {
                        String errMsg = "HashStoreClient - objType must be 'object' or 'metadata'";
                        throw new IllegalArgumentException(errMsg);
                    }
                }
                Path setItemFilePath = Paths.get(originDir + "/" + docid + "." + rev);

                if (Files.exists(setItemFilePath)) {
                    System.out.println(
                        "File exists (" + setItemFilePath + ")! Adding to resultObjList."
                    );
                    Map<String, String> resultObj = new HashMap<>();
                    resultObj.put("pid", guid);
                    resultObj.put("algorithm", formattedChecksumAlgo);
                    resultObj.put("checksum", checksum);
                    resultObj.put("path", setItemFilePath.toString());
                    resultObj.put("namespace", formatId);
                    resultObjList.add(resultObj);
                }
            }

            // Check options
            if (actionFlag.equals("sts") && objType.equals("object")) {
                storeObjsWithChecksumFromDb(resultObjList);
            }
            if (actionFlag.equals("sts") && objType.equals("metadata")) {
                storeMetadataFromDb(resultObjList);
            }
            if (actionFlag.equals("rav") && objType.equals("object")) {
                retrieveAndValidateObjs(resultObjList);
            }
            if (actionFlag.equals("rav") && objType.equals("metadata")) {
                retrieveAndValidateMetadata(resultObjList);
            }
            if (actionFlag.equals("dfs") && objType.equals("object")) {
                deleteObjectsFromStore(resultObjList);
            }
            if (actionFlag.equals("dfs") && objType.equals("metadata")) {
                deleteMetadataFromStore(resultObjList);
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
     * Store objects to a HashStore with a checksum and checksum algorithm
     * 
     * @param resultObjList List containing items with the following properties: 'pid', 'path',
     *                      'algorithm', 'checksum'
     */
    private static void storeObjsWithChecksumFromDb(List<Map<String, String>> resultObjList) {
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
                String errMsg = "Unexpected Error: " + poee.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_obj_errors/pidobjectexists");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IllegalArgumentException iae) {
                String errMsg = "Unexpected Error: " + iae.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_obj_errors/illegalargument");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_obj_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_obj_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * Retrieve objects from a HashStore and validate its contents by comparing checksums.
     * 
     * @param resultObjList List containing items with the following properties: 'pid', 'algorithm',
     *                      'checksum'
     */
    private static void retrieveAndValidateObjs(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                String algorithm = item.get("algorithm");
                String checksum = item.get("checksum");

                // Retrieve object
                System.out.println("Retrieving object for guid: " + guid);
                InputStream objStream = hashStore.retrieveObject(guid);

                // Get hex digest
                System.out.println("Calculating hex digest with algorithm: " + algorithm);
                String streamDigest = calculateHexDigest(objStream, algorithm);
                objStream.close();

                // If checksums don't match, write a .txt file
                if (!streamDigest.equals(checksum)) {
                    String errMsg = "Object retrieved (pid/guid): " + guid
                        + ". Checksums do not match, checksum from db: " + checksum
                        + ". Calculated digest: " + streamDigest + ". Algorithm: " + algorithm;
                    logExceptionToFile(guid, errMsg, "java/retrieve_obj_errors/checksum_mismatch");
                } else {
                    System.out.println("Checksums match!");
                }

            } catch (FileNotFoundException fnfe) {
                String errMsg = "File not found: " + fnfe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_obj_errors/filenotfound");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_obj_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_obj_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * Deletes a list of objects from a HashStore
     * 
     * @param resultObjList List containing items with the following property: 'pid'
     */
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
                    logExceptionToFile(guid, errMsg, "java/delete_obj_errors/filenotfound");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_obj_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_obj_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * Store a list containing info about metadata to a HashStore
     * 
     * @param resultObjList List containing items that have the following properties: 'pid', 'path'
     *                      and 'namespace'
     */
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

            } catch (IllegalArgumentException iae) {
                String errMsg = "Unexpected Error: " + iae.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_metadata_errors/illegalargument");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_metadata_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/store_metadata_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * Retrieve metadata from a HashStore and validate its contents by comparing checksums.
     * 
     * @param resultObjList List containing items with the following properties: 'pid', 'namespace',
     *                      'algorithm', 'checksum'
     */
    private static void retrieveAndValidateMetadata(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                String algorithm = item.get("algorithm");
                String checksum = item.get("checksum");
                String formatId = item.get("namespace");

                // Retrieve object
                System.out.println("Retrieving metadata for guid: " + guid);
                InputStream metadataStream = hashStore.retrieveMetadata(guid, formatId);

                // Get hex digest
                System.out.println("Calculating hex digest with algorithm: " + algorithm);
                String streamDigest = calculateHexDigest(metadataStream, algorithm);
                metadataStream.close();

                // If checksums don't match, write a .txt file
                if (!streamDigest.equals(checksum)) {
                    String errMsg = "Metadata retrieved (pid/guid): " + guid
                        + ". Checksums do not match, checksum from db: " + checksum
                        + ". Calculated digest: " + streamDigest + ". Algorithm: " + algorithm;
                    logExceptionToFile(
                        guid, errMsg, "java/retrieve_metadata_errors/checksum_mismatch"
                    );
                } else {
                    System.out.println("Checksums match!");
                }

            } catch (FileNotFoundException fnfe) {
                String errMsg = "File not found: " + fnfe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_metadata_errors/filenotfound");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_metadata_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/retrieve_metadata_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }

    /**
     * Deletes a list of metadata from a HashStore
     * 
     * @param resultObjList List containing items with the following property: 'pid'
     */
    private static void deleteMetadataFromStore(List<Map<String, String>> resultObjList) {
        resultObjList.parallelStream().forEach(item -> {
            String guid = null;
            try {
                guid = item.get("pid");
                String formatId = item.get("namespace");

                // Delete object
                System.out.println("Deleting metadata for guid: " + guid);
                hashStore.deleteMetadata(guid, formatId);

            } catch (FileNotFoundException fnfe) {
                String errMsg = "Unexpected Error: " + fnfe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_metadata_errors/filenotfound");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (IOException ioe) {
                String errMsg = "Unexpected Error: " + ioe.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_metadata_errors/io");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } catch (Exception ge) {
                String errMsg = "Unexpected Error: " + ge.fillInStackTrace();
                try {
                    logExceptionToFile(guid, errMsg, "java/delete_metadata_errors/general");
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        });
    }


    // Utility methods

    /**
     * Checks whether a given object is null and throws an exception if so
     *
     * @param object   Object to check
     * @param argument Value that is being checked
     */
    private static void ensureNotNull(Object object, String argument) {
        if (object == null) {
            String errMsg = "HashStoreClient - " + argument + " cannot be null.";
            throw new NullPointerException(errMsg);
        }
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

    /**
     * Log a plain text file with the guid/pid as the file name with a message.
     * 
     * @param guid      Pid/guid for which an exception was encountered.
     * @param errMsg    Message to write into text file.
     * @param directory Directory within HashStore to log error (txt) files.
     * @throws Exception Catch all for unexpected exceptions
     */
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
}

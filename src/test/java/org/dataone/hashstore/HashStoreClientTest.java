package org.dataone.hashstore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import org.dataone.hashstore.filehashstore.FileHashStoreUtility;
import org.dataone.hashstore.testdata.TestDataHarness;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HashStoreClientTest {
    private static HashStore hashStore;
    private static final TestDataHarness testData = new TestDataHarness();
    private Properties hsProperties;

    /**
     * Temporary folder for tests to run in
     */
    @TempDir
    public Path tempFolder;

    @BeforeEach
    public void getHashStore() {
        String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
        Path rootDirectory = tempFolder.resolve("hashstore");

        Properties storeProperties = new Properties();
        storeProperties.setProperty("storePath", rootDirectory.toString());
        storeProperties.setProperty("storeDepth", "3");
        storeProperties.setProperty("storeWidth", "2");
        storeProperties.setProperty("storeAlgorithm", "SHA-256");
        storeProperties.setProperty(
            "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata");

        try {
            hsProperties = storeProperties;
            hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);

        } catch (Exception e) {
            e.printStackTrace();
            fail("ClientTest - Exception encountered: " + e.getMessage());

        }
    }

    /**
     * Generates a hierarchical path by dividing a given digest into tokens of fixed width, and
     * concatenating them with '/' as the delimiter.
     *
     * @param dirDepth integer to represent number of directories
     * @param dirWidth width of each directory
     * @param digest   value to shard
     * @return String
     */
    protected String getHierarchicalPathString(int dirDepth, int dirWidth, String digest) {
        Collection<String> tokens = new ArrayList<>();
        int digestLength = digest.length();
        for (int i = 0; i < dirDepth; i++) {
            int start = i * dirWidth;
            int end = Math.min((i + 1) * dirWidth, digestLength);
            tokens.add(digest.substring(start, end));
        }

        if (dirDepth * dirWidth < digestLength) {
            tokens.add(digest.substring(dirDepth * dirWidth));
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
     * Utility method to get absolute path of a given object and objType ("objects", "metadata",
     * "cid", or "pid").
     */
    public Path getObjectAbsPath(String id, String objType) throws Exception {
        String storeAlgo = hsProperties.getProperty("storeAlgorithm");
        int shardDepth = Integer.parseInt(hsProperties.getProperty("storeDepth"));
        int shardWidth = Integer.parseInt(hsProperties.getProperty("storeWidth"));
        // Get absolute path
        Path storePath = Paths.get(hsProperties.getProperty("storePath"));
        Path absPath = null;
        if (objType.equals("object")) {
            // Get relative path
            String objCidShardString = getHierarchicalPathString(shardDepth, shardWidth, id);
            absPath = storePath.resolve("objects/" + objCidShardString);
        }
        if (objType.equals("metadata")) {
            // Get pid metadata directory hash(pid)
            String pidHash = FileHashStoreUtility.getPidHexDigest(id, storeAlgo);
            String pidMetadataDirectory =
                getHierarchicalPathString(shardDepth, shardWidth, pidHash);
            // Get sysmeta name hash(pid+default_formatId)
            String metadataDocHash = FileHashStoreUtility.getPidHexDigest(
                id + hsProperties.getProperty("storeMetadataNamespace"), storeAlgo);
            absPath = storePath.resolve("metadata").resolve(pidMetadataDirectory)
                .resolve(metadataDocHash);
        }
        if (objType.equals("cid")) {
            String pidRelativePath =
                FileHashStoreUtility.getHierarchicalPathString(shardDepth, shardWidth, id);
            absPath = storePath.resolve("refs/cids").resolve(pidRelativePath);
        }
        if (objType.equals("pid")) {
            String hashId = FileHashStoreUtility.getPidHexDigest(id, storeAlgo);
            String pidRelativePath =
                FileHashStoreUtility.getHierarchicalPathString(shardDepth, shardWidth, hashId);
            absPath = storePath.resolve("refs/pids").resolve(pidRelativePath);
        }
        return absPath;
    }

    /**
     * Test creating a HashStore through client.
     */
    @Test
    public void client_createHashStore() throws Exception {
        String optCreateHashstore = "-chs";
        String optStore = "-store";
        String optStorePath = tempFolder + "/hashstore";
        String optStoreDepth = "-dp";
        String optStoreDepthValue = "3";
        String optStoreWidth = "-wp";
        String optStoreWidthValue = "2";
        String optAlgo = "-ap";
        String optAlgoValue = "SHA-256";
        String optFormatId = "-nsp";
        String optFormatIdValue = "https://ns.dataone.org/service/types/v2.0#SystemMetadata";
        String[] args =
            {optCreateHashstore, optStore, optStorePath, optStoreDepth, optStoreDepthValue,
                optStoreWidth, optStoreWidthValue, optAlgo, optAlgoValue, optFormatId,
                optFormatIdValue};
        HashStoreClient.main(args);

        Path storePath = Paths.get(optStorePath);
        Path hashStoreObjectsPath = storePath.resolve("objects");
        Path hashStoreMetadataPath = storePath.resolve("metadata");
        Path hashStoreYaml = storePath.resolve("hashstore.yaml");
        System.out.println(hashStoreYaml);
        assertTrue(Files.exists(hashStoreObjectsPath));
        assertTrue(Files.exists(hashStoreMetadataPath));
        assertTrue(Files.exists(hashStoreYaml));
    }

    /**
     * Test hashStore client stores objects.
     */
    @Test
    public void client_storeObjects() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Call client
            String optStoreObject = "-storeobject";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPath = "-path";
            String optObjectPath = testDataFile.toString();
            String optPid = "-pid";
            String[] args =
                {optStoreObject, optStore, optStorePath, optPath, optObjectPath, optPid, pid};
            HashStoreClient.main(args);

            // Confirm object was stored
            Path absPath = getObjectAbsPath(testData.pidData.get(pid).get("sha256"), "object");
            assertTrue(Files.exists(absPath));

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client stores metadata.
     */
    @Test
    public void client_storeMetadata() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            // Call client
            String optStoreMetadata = "-storemetadata";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPath = "-path";
            String optObjectPath = testDataFile.toString();
            String optPid = "-pid";
            String optFormatId = "-format_id";
            String optFormatIdValue = hsProperties.getProperty("storeMetadataNamespace");
            String[] args =
                {optStoreMetadata, optStore, optStorePath, optPath, optObjectPath, optPid, pid,
                    optFormatId, optFormatIdValue};
            HashStoreClient.main(args);

            // Confirm metadata was stored
            // Calculate absolute path
            String storeAlgorithm = hsProperties.getProperty("storeAlgorithm");
            int storeDepth = Integer.parseInt(hsProperties.getProperty("storeDepth"));
            int storeWidth = Integer.parseInt(hsProperties.getProperty("storeWidth"));
            Path metadataDirectory =
                Paths.get(hsProperties.getProperty("storePath")).resolve("metadata");
            String metadataCidPartOne = FileHashStoreUtility.getPidHexDigest(pid, storeAlgorithm);
            String pidMetadataDirectory =
                FileHashStoreUtility.getHierarchicalPathString(storeDepth, storeWidth,
                                                               metadataCidPartOne);
            // The file name for the metadata document is the hash of the supplied 'formatId'
            String metadataCidPartTwo =
                FileHashStoreUtility.getPidHexDigest(pid + optFormatIdValue, storeAlgorithm);
            Path expectedMetadataPath =
                metadataDirectory.resolve(pidMetadataDirectory).resolve(metadataCidPartTwo);
            assertTrue(Files.exists(expectedMetadataPath));

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client retrieves objects.
     */
    @Test
    public void client_retrieveObjects() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                hashStore.storeObject(dataStream, pid, null, null, null, -1);
            }

            // Call client
            String optRetrieveObject = "-retrieveobject";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPid = "-pid";
            String[] args = {optRetrieveObject, optStore, optStorePath, optPid, pid};
            HashStoreClient.main(args);

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client retrieves objects.
     */
    @Test
    public void client_retrieveMetadata() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                hashStore.storeMetadata(metadataStream, pid);
            }

            // Call client
            String optRetrieveMetadata = "-retrievemetadata";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPid = "-pid";
            String optFormatId = "-format_id";
            String optFormatIdValue = hsProperties.getProperty("storeMetadataNamespace");
            String[] args = {optRetrieveMetadata, optStore, optStorePath, optPid, pid, optFormatId,
                optFormatIdValue};
            HashStoreClient.main(args);

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client deletes objects.
     */
    @Test
    public void client_deleteObjects() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                hashStore.storeObject(dataStream, pid, null, null, null, -1);
            }

            // Call client
            String optDeleteObject = "-deleteobject";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPid = "-pid";
            String[] args = {optDeleteObject, optStore, optStorePath, optPid, pid};
            HashStoreClient.main(args);

            // Confirm object was deleted
            Path absPath = getObjectAbsPath(testData.pidData.get(pid).get("sha256"), "object");
            assertFalse(Files.exists(absPath));

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client retrieves objects.
     */
    @Test
    public void client_deleteMetadata() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testMetaDataFile = testData.getTestFile(pidFormatted + ".xml");

            try (InputStream metadataStream = Files.newInputStream(testMetaDataFile)) {
                hashStore.storeMetadata(metadataStream, pid);
            }

            // Call client
            String optDeleteMetadata = "-deletemetadata";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPid = "-pid";
            String optFormatId = "-format_id";
            String optFormatIdValue = hsProperties.getProperty("storeMetadataNamespace");
            String[] args = {optDeleteMetadata, optStore, optStorePath, optPid, pid, optFormatId,
                optFormatIdValue};
            HashStoreClient.main(args);

            // Confirm metadata was deleted
            Path sysmetaPath = getObjectAbsPath(pid, "metadata");
            assertFalse(Files.exists(sysmetaPath));

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm client printed content
            String pidStdOut = outputStream.toString();
            assertFalse(pidStdOut.isEmpty());
        }
    }

    /**
     * Test hashStore client calculates the hex digest of object.
     */
    @Test
    public void client_getHexDigest() throws Exception {
        for (String pid : testData.pidList) {
            // Redirect stdout to capture output
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream(outputStream);
            PrintStream old = System.out;
            System.setOut(ps);

            String pidFormatted = pid.replace("/", "_");
            Path testDataFile = testData.getTestFile(pidFormatted);

            try (InputStream dataStream = Files.newInputStream(testDataFile)) {
                hashStore.storeObject(dataStream, pid, null, null, null, -1);
            }

            // Call client
            String optGetChecksum = "-getchecksum";
            String optStore = "-store";
            String optStorePath = hsProperties.getProperty("storePath");
            String optPid = "-pid";
            String optAlgo = "-algo";
            String optAlgoValue = "SHA-256";
            String[] args =
                {optGetChecksum, optStore, optStorePath, optPid, pid, optAlgo, optAlgoValue};
            HashStoreClient.main(args);


            String testDataChecksum = testData.pidData.get(pid).get("sha256");

            // Put things back
            System.out.flush();
            System.setOut(old);

            // Confirm hex digest matches
            String pidStdOut = outputStream.toString();
            assertEquals(testDataChecksum, pidStdOut.trim());
        }
    }
}

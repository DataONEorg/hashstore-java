package org.dataone.hashstore;

import java.io.BufferedWriter;
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
import java.util.HashMap;
import java.util.Map;
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
            Path storePath = Paths.get("/home/mok/testing/test_all");

            // Initialize HashStore
            String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";

            Properties storeProperties = new Properties();
            storeProperties.setProperty("storePath", storePath.toString());
            storeProperties.setProperty("storeDepth", "3");
            storeProperties.setProperty("storeWidth", "2");
            storeProperties.setProperty("storeAlgorithm", "SHA-256");
            storeProperties.setProperty("storeMetadataNamespace", "http://www.ns.test/v1");


            // Get HashStore
            hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);

            // Get file names from `var/metacat/data`
            // String originalObjDirectory = "/var/metacata/data";
            // Path originalObjDirectoryPath = Paths.get(originalObjDirectory);
            // File[] storePathFileList = storePath.toFile().listFiles();

            Files.createDirectories(Paths.get("/home/mok/testing/test_all/douyamlcheck"));

            // for (int i = 0; i < storePathFileList.length - 1; i++) {
            for (int i = 0; i < 100; i++) {
                String pid = "dou.test." + i;

                try {
                    InputStream pidObjStream = hashStore.retrieveObject(pid);
                    Map<String, String> hexDigests = generateChecksums(pidObjStream);
                    String yamlObjectString = getHexDigestsYamlString(
                        hexDigests.get("MD5"), hexDigests.get("SHA-1"), hexDigests.get("SHA-256"),
                        hexDigests.get("SHA-384"), hexDigests.get("SHA-512")
                    );

                    Path pidObjectYaml = Paths.get("/home/mok/testing/test_all/douyamlcheck")
                        .resolve(pid + ".yaml");

                    try (BufferedWriter writer = new BufferedWriter(
                        new OutputStreamWriter(
                            Files.newOutputStream(pidObjectYaml), StandardCharsets.UTF_8
                        )
                    )) {
                        writer.write(yamlObjectString);

                    } catch (Exception e) {
                        e.fillInStackTrace();
                    }

                } catch (FileNotFoundException fnfe) {
                    fnfe.fillInStackTrace();
                } catch (IOException ioe) {
                    ioe.fillInStackTrace();
                } catch (IllegalArgumentException iae) {
                    iae.fillInStackTrace();
                } catch (NoSuchAlgorithmException nsae) {
                    nsae.fillInStackTrace();
                }
            }

        } catch (Exception e) {
            e.fillInStackTrace();
        }

    }

    private static String getHexDigestsYamlString(
        String md5digest, String sha1digest, String sha256digest, String sha384digest,
        String sha512digest
    ) {
        return String.format(
            "md5digest:\n" + "- %s\n\n" + "sha1digest:\n" + "- %s\n\n" + "sha256digest:\n"
                + "- %s\n\n" + "sha384digest:\n" + "- %s\n\n" + "sha512digest:\n" + "- %s\n\n",
            md5digest, sha1digest, sha256digest, sha384digest, sha512digest
        );
    }

    private static Map<String, String> generateChecksums(InputStream pidObjStream)
        throws NoSuchAlgorithmException {
        MessageDigest md5 = MessageDigest.getInstance(DefaultHashAlgorithms.MD5.getName());
        MessageDigest sha1 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_1.getName());
        MessageDigest sha256 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_256.getName());
        MessageDigest sha384 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_384.getName());
        MessageDigest sha512 = MessageDigest.getInstance(DefaultHashAlgorithms.SHA_512.getName());

        try {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = pidObjStream.read(buffer)) != -1) {
                md5.update(buffer, 0, bytesRead);
                sha1.update(buffer, 0, bytesRead);
                sha256.update(buffer, 0, bytesRead);
                sha384.update(buffer, 0, bytesRead);
                sha512.update(buffer, 0, bytesRead);
            }

        } catch (Exception e) {
            e.fillInStackTrace();
        }

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

        return hexDigests;
    }
}

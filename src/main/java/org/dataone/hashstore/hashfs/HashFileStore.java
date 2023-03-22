package org.dataone.hashstore.hashfs;

public class HashFileStore {
    private byte directoryDepth;
    private byte directoryWidth;
    private String rootDirectory = System.getProperty("user.dir");
    private String algorithm = "sha256";

    public HashFileStore(byte depth, byte width) {
        this.directoryDepth = depth;
        this.directoryWidth = width;
    }
}
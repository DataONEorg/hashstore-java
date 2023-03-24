package org.dataone.hashstore.hashfs;

import java.util.Map;

public class HashAddress {
    private String id;
    private String relPath;
    private String absPath;
    private boolean isDuplicate;
    private Map<String, String> hexDigests;

    public HashAddress(String id, String relPath, String absPath, boolean isDuplicate,
            Map<String, String> hexDigests) {
        this.id = id;
        this.relPath = relPath;
        this.absPath = absPath;
        this.isDuplicate = isDuplicate;
        this.hexDigests = hexDigests;
    }

    public String getId() {
        return id;
    }

    public String getRelPath() {
        return relPath;
    }

    public String getAbsPath() {
        return absPath;
    }

    public boolean getIsDuplicate() {
        return isDuplicate;
    }

    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}
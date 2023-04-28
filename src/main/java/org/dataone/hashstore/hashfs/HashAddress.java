package org.dataone.hashstore.hashfs;

import java.util.Map;

public class HashAddress {
    private String id;
    private String relPath;
    private String absPath;
    private boolean isNotDuplicate;
    private Map<String, String> hexDigests;

    public HashAddress(String id, String relPath, String absPath, boolean isNotDuplicate,
            Map<String, String> hexDigests) {
        this.id = id;
        this.relPath = relPath;
        this.absPath = absPath;
        this.isNotDuplicate = isNotDuplicate;
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

    public boolean getIsNotDuplicate() {
        return isNotDuplicate;
    }

    public Map<String, String> getHexDigests() {
        return hexDigests;
    }
}
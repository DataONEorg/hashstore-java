package org.dataone.hashstore;

import java.util.Map;

/**
 * ObjectMetadata is a record that that contains metadata about an object in the HashStore. It
 * encapsulates information about a file's authority-based/persistent identifier (pid), content
 * identifier (cid), size, and associated hash digest values.
 */
public record ObjectMetadata(String pid, String cid, long size, Map<String, String> hexDigests) {

}
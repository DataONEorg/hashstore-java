package org.dataone.hashstore.interfaces;

import java.io.InputStream;
import org.dataone.hashstore.hashfs.HashAddress;

public interface HashStoreInterface {
    public HashAddress storeObject(InputStream object, String pid, String additionalAlgorithm, String checksum,
            String checksumAlgorithm) throws Exception;
}

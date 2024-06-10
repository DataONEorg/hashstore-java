package org.dataone.hashstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

/**
 * A HashStoreServiceRequest represents the data needed for a single request to HashStore
 * packaged as a Runnable task that can be executed within a thread pool, typically
 * provided by the Executor service.
 */
public class HashStoreServiceRequest {
    public static final int storeObject = 1;
    public static final int deleteObject = 2;
    private HashStore hashstore = null;
    private int publicAPIMethod;

    private static final Log logHssr = LogFactory.getLog(HashStoreServiceRequest.class);

    protected HashStoreServiceRequest(HashStore hashstore, int publicAPIMethod) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore",
                                           "HashStoreServiceRequestConstructor");
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
    }

    public void run() {
        logHssr.debug("HashStoreServiceRequest - Called to: " + publicAPIMethod);
    }
}

package org.dataone.hashstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.exceptions.HashStoreServiceException;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

import java.io.InputStream;

/**
 * A HashStoreRunnable represents the data needed for a single request to HashStore
 * packaged as a Runnable task that can be executed within a thread pool, typically
 * provided by the Executor service.
 */
public class HashStoreRunnable implements Runnable {
    public static final int storeObject = 1;
    public static final int deleteObject = 2;
    private HashStore hashstore = null;
    private int publicAPIMethod;
    private String pid;
    private InputStream objStream;

    private static final Log logHssr = LogFactory.getLog(HashStoreRunnable.class);

    public HashStoreRunnable(HashStore hashstore, int publicAPIMethod, InputStream objStream,
                             String pid) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore",
                                           "HashStoreServiceRequestConstructor");
        FileHashStoreUtility.checkNotNegativeOrZero(publicAPIMethod, "HashStoreServiceRequestConstructor");
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
        this.objStream = objStream;
        this.pid = pid;
    }

    public void run() {
        logHssr.debug("HashStoreServiceRequest - Called to: " + publicAPIMethod);
        try {
            switch (publicAPIMethod) {
                case storeObject:
                    try {
                        hashstore.storeObject(objStream, pid, null, null, null, -1);
                    } catch (Exception e) {
                        throw new HashStoreServiceException(e.getMessage());
                    }
                    break;
                case deleteObject:
                    try {
                        hashstore.deleteObject("pid", pid);
                    } catch (Exception e) {
                        throw new HashStoreServiceException(e.getMessage());
                    }
                    break;
            }
        } catch (HashStoreServiceException hse) {
            logHssr.error("HashStoreServiceRequest - Error: " + hse.getMessage());
        }
    }
}

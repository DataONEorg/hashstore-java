package org.dataone.hashstore;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.exceptions.HashStoreServiceException;
import org.dataone.hashstore.filehashstore.FileHashStoreUtility;

import java.io.IOException;
import java.io.InputStream;

/**
 * A HashStoreRunnable represents the data needed for a single request to HashStore
 * packaged as a Runnable task that can be executed within a thread pool, typically
 * provided by the Executor service.
 */
public class HashStoreRunnable implements Runnable {
    private static final Log log = LogFactory.getLog(HashStoreRunnable.class);
    public static final int storeObject = 1;
    public static final int deleteObject = 2;
    private final HashStore hashstore;
    private final int publicAPIMethod;
    private final String pid;
    private InputStream objStream;

    public HashStoreRunnable(HashStore hashstore, int publicAPIMethod, InputStream objStream,
                             String pid) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore",
                                           "HashStoreServiceRequestConstructor");
        FileHashStoreUtility.checkPositive(publicAPIMethod, "HashStoreServiceRequestConstructor");
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
        this.objStream = objStream;
        this.pid = pid;
    }

    public HashStoreRunnable(HashStore hashstore, int publicAPIMethod, String pid) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore",
                                           "HashStoreServiceRequestConstructor");
        FileHashStoreUtility.checkPositive(publicAPIMethod, "HashStoreServiceRequestConstructor");
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
        this.pid = pid;
    }

    public void run() {
        log.debug("HashStoreServiceRequest - Called to: " + publicAPIMethod);
        try {
            switch (publicAPIMethod) {
                case storeObject:
                    try {
                        hashstore.storeObject(objStream, pid, null, null, null, -1);
                    } catch (Exception e) {
                        String errMsg =
                            "HashStoreRunnable ~ UnexpectedError - storeObject: " + e.getCause();
                        System.out.println(errMsg);
                        log.error(errMsg);
                        throw new HashStoreServiceException(errMsg);
                    }
                    objStream.close();
                    break;
                case deleteObject:
                    try {
                        hashstore.deleteObject(pid);
                    } catch (Exception e) {
                        String errMsg =
                            "HashStoreRunnable ~ UnexpectedError - deleteObject: " + e.getCause();
                        System.out.println(errMsg);
                        log.error(errMsg);
                        throw new HashStoreServiceException(errMsg);
                    }
                    break;
            }
        } catch (HashStoreServiceException | IOException hse) {
            log.error(
                "HashStoreServiceRequest ~ Unexpected Error: " + hse.getMessage());
        }
    }
}

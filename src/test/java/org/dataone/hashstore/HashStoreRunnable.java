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

    /**
     * Constructor for HashStoreRunnable to store a data object with a given pid
     *
     * @param hashstore       HashStore object to interact with
     * @param publicAPIMethod Integer representing action/Public API method (ex. 1 for storeObject)
     * @param objStream       Stream to data object
     * @param pid             Persistent or authority-based identifier
     */
    public HashStoreRunnable(HashStore hashstore, int publicAPIMethod, InputStream objStream,
                             String pid) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore");
        FileHashStoreUtility.checkPositive(publicAPIMethod);
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
        this.objStream = objStream;
        this.pid = pid;
    }

    /**
     * Constructor for HashStoreRunnable where only a pid is necessary (ex. to delete an object).
     *
     * @param hashstore       HashStore object to interact with
     * @param publicAPIMethod Integer representing action/Public API method (ex. 2 for deleteObject)
     * @param pid             Persistent or authority-based identifier
     */
    public HashStoreRunnable(HashStore hashstore, int publicAPIMethod, String pid) {
        FileHashStoreUtility.ensureNotNull(hashstore, "hashstore");
        FileHashStoreUtility.checkPositive(publicAPIMethod);
        this.hashstore = hashstore;
        this.publicAPIMethod = publicAPIMethod;
        this.pid = pid;
    }

    /**
     * Executes a HashStore action (ex. storeObject, deleteObject)
     */
    public void run() {
        log.debug("HashStoreRunnable - Called to: " + publicAPIMethod);
        try {
            switch (publicAPIMethod) {
                case storeObject -> {
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
                }
                case deleteObject -> {
                    try {
                        hashstore.deleteObject(pid);
                    } catch (Exception e) {
                        String errMsg =
                            "HashStoreRunnable ~ UnexpectedError - deleteObject: " + e.getCause();
                        System.out.println(errMsg);
                        log.error(errMsg);
                        throw new HashStoreServiceException(errMsg);
                    }
                }
            }
        } catch (HashStoreServiceException | IOException hse) {
            log.error(
                "HashStoreRunnable ~ Unexpected Error: " + hse.getMessage());
        }
    }
}

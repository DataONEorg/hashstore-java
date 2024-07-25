package org.dataone.hashstore.hashstoreconverter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dataone.hashstore.filehashstore.FileHashStore;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class FileHashStoreLinks extends FileHashStore {

    private static final Log logFileHashStoreLinks = LogFactory.getLog(FileHashStore.class);

    public FileHashStoreLinks(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        super(hashstoreProperties);
        logFileHashStoreLinks.info("FileHashStoreLinks initialized");
    }

}

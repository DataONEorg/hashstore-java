package org.dataone.hashstore.hashstoreconverter;

import org.dataone.hashstore.filehashstore.FileHashStore;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;

public class FileHashStoreLinks extends FileHashStore {

    public FileHashStoreLinks(Properties hashstoreProperties) throws IllegalArgumentException,
        IOException, NoSuchAlgorithmException {
        super(hashstoreProperties);
    }

}

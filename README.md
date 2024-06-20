## HashStore: hash-based object storage for DataONE data packages

- **Author**: Matthew B. Jones, Dou Mok, Jing Tao, Matthew Brooke
- **License**: [Apache 2](http://opensource.org/licenses/Apache-2.0)
- [Package source code on GitHub](https://github.com/DataONEorg/hashstore-java)
- [**Submit Bugs and feature requests**](https://github.com/DataONEorg/hashstore-java/issues)
- Contact us: support@dataone.org
- [DataONE discussions](https://github.com/DataONEorg/dataone/discussions)

HashStore is a server-side java library that implements an object storage file system for storing and accessing data and metadata for DataONE services. The package is used in DataONE system components that need direct, filesystem-based access to data objects, their system metadata, and extended metadata about the objects. This package is a core component of the [DataONE federation](https://dataone.org), and supports large-scale object storage for a variety of repositories, including the [KNB Data Repository](http://knb.ecoinformatics.org), the [NSF Arctic Data Center](https://arcticdata.io/catalog/), the [DataONE search service](https://search.dataone.org), and other repositories.

DataONE in general, and HashStore in particular, are open source, community projects.  We [welcome contributions](https://github.com/DataONEorg/hashstore-java/blob/main/CONTRIBUTING.md) in many forms, including code, graphics, documentation, bug reports, testing, etc.  Use the [DataONE discussions](https://github.com/DataONEorg/dataone/discussions) to discuss these contributions with us.

## Documentation

Documentation is a work in progress, and can be found on the [Metacat repository](https://github.com/NCEAS/metacat/blob/feature-1436-storage-and-indexing/docs/user/metacat/source/storage-subsystem.rst#physical-file-layout) as part of the storage redesign planning. Future updates will include documentation here as the package matures.

## HashStore Overview

HashStore is an object storage system that provides persistent file-based storage using content hashes to de-duplicate data. The system stores both objects, references (refs) and metadata in its respective directories and utilizes an identifier-based API for interacting with the store. HashStore storage classes (like `FileHashStore`) must implement the HashStore interface to ensure the expected usage of HashStore.

###### Public API Methods
- storeObject
- verifyObject
- tagObject
- findObject
- storeMetadata
- retrieveObject
- retrieveMetadata
- deleteObject
- deleteMetadata
- getHexDigest

For details, please see the HashStore interface (HashStore.java)


###### How do I create a HashStore?

To create or interact with a HashStore, instantiate a HashStore object with the following set of properties:
- storePath
- storeDepth
- storeWidth
- storeAlgorithm
- storeMetadataNamespace

```java
String classPackage = "org.dataone.hashstore.filehashstore.FileHashStore";
Path rootDirectory = tempFolder.resolve("metacat");

Properties storeProperties = new Properties();
storeProperties.setProperty("storePath", rootDirectory.toString());
storeProperties.setProperty("storeDepth", "3");
storeProperties.setProperty("storeWidth", "2");
storeProperties.setProperty("storeAlgorithm", "SHA-256");
storeProperties.setProperty(
    "storeMetadataNamespace", "https://ns.dataone.org/service/types/v2.0#SystemMetadata"
);

// Instantiate a HashStore
HashStore hashStore = HashStoreFactory.getHashStore(classPackage, storeProperties);

// Store an object
hashStore.storeObject(stream, pid)
// ...
```


###### Working with objects (store, retrieve, delete)

In HashStore, objects are first saved as temporary files while their content identifiers are calculated. Once the default hash algorithm list and their hashes are generated, objects are stored in their permanent location using the store's algorithm's corresponding hash value, the store depth and the store width. Lastly, reference files are created for the object so that they can be found and retrieved given an identifier (ex. persistent identifier (pid)). Note: Objects are also stored once and only once.

By calling the various interface methods for  `storeObject`, the calling app/client can validate, store and tag an object simultaneously if the relevant data is available. In the absence of an identifier (ex. persistent identifier (pid)), `storeObject` can be called to solely store an object. The client is then expected to call `verifyObject` when the relevant metadata is available to confirm that the object has been stored as expected. And to finalize the process (to make the object discoverable), the client calls `tagObject``. In summary, there are two expected paths to store an object:
```java
// All-in-one process which stores, validates and tags an object
objectMetadata objInfo = storeObject(InputStream, pid, additionalAlgorithm, checksum, checksumAlgorithm, objSize)

// Manual Process
// Store object
objectMetadata objInfo = storeObject(InputStream)
// Validate object, returns False if there is a mismatch and deletes the associated file
verifyObject(objInfo, checksum, checksumAlgorithn, objSize)
// Tag object, makes the object discoverable (find, retrieve, delete)
tagObject(pid, cid)
```

**How do I retrieve an object if I have the pid?**
- To retrieve an object, call the Public API method `retrieveObject` which opens a stream to the object if it exists.

**How do I find an object or check that it exists if I have the pid?**
- To check if an object exists, call the Public API method `findObject` which will return the content identifier (cid) of the object if it exists.
- If desired, this cid can then be used to locate the object on disk by following HashStore's store configuration.

**How do I delete an object if I have the pid?**
- To delete an object, all its associated reference files and its metadata, call the Public API method `deleteObject()` with `idType` 'pid'. If an `idType` is not given (ex. calling `deleteObject(String pid)`), the `idType` will be assumed to be a 'pid'
- To delete only an object, call `deleteObject()` with `idType` 'cid' which will remove the object if it is not referenced by any pids.
- Note, `deleteObject` and `tagObject` calls are synchronized on their content identifier values so that the shared reference files are not unintentionally modified concurrently. An object that is in the process of being deleted should not be tagged, and vice versa. These calls have been implemented to occur sequentially to improve clarity in the event of an unexpected conflict or issue.


###### Working with metadata (store, retrieve, delete)

HashStore's '/metadata' directory holds all metadata for objects stored in HashStore. All metadata documents related to a 'pid' are stored in a directory determined by calculating the hash of the pid (based on the store's algorithm). Each specific metadata document is then stored by calculating the hash of its associated `pid+formatId`. By default, calling `storeMetadata` will use HashStore's default metadata namespace as the 'formatId' when storing metadata. Should the calling app wish to store multiple metadata files about an object, the client app is expected to provide a 'formatId' that represents an object format for the metadata type (ex. `storeMetadata(stream, pid, formatId)`). 

**How do I retrieve a metadata file?**
- To find a metadata object, call the Public API method `retrieveMetadata` which returns a stream to the metadata file that's been stored with the default metadata namespace if it exists.
- If there are multiple metadata objects, a 'formatId' must be specified when calling `retrieveMetadata` (ex. `retrieveMetadata(pid, formatId)`)

**How do I delete a metadata file?**
- Like `retrieveMetadata`, call the Public API method `deleteMetadata(String pid, String formatId)` which will delete the metadata object associated with the given pid.
- To delete all metadata objects related to a given 'pid', call `deleteMetadata(String pid)`


###### What are HashStore reference files?

HashStore assumes that every object to store has a respective identifier. This identifier is then used when storing, retrieving and deleting an object. In order to facilitate this process, we create two types of reference files:
- pid (persistent identifier) reference files 
- cid (content identifier) reference files

These reference files are implemented in HashStore underneath the hood with no expectation for modification from the calling app/client. The one and only exception to this process is when the calling client/app does not have an identifier, and solely stores an objects raw bytes in HashStore (calling `storeObject(InputStream)`).

**'pid' Reference Files**
- Pid (persistent identifier) reference files are created when storing an object with an identifier.
- Pid reference files are located in HashStores '/refs/pid' directory
- If an identifier is not available at the time of storing an object, the calling app/client must create this association between a pid and the object it represents by calling `tagObject` separately.
- Each pid reference file contains a string that represents the content identifier of the object it references
- Like how objects are stored once and only once, there is also only one pid reference file for each object.

**'cid' Reference Files**
- Cid (content identifier) reference files are created at the same time as pid reference files when storing an object with an identifier.
- Cid reference files are located in HashStore's '/refs/cid' directory
- A cid reference file is a list of all the pids that reference a cid, delimited by a new line ("\n") character


###### What does HashStore look like?

```sh
# Example layout in HashStore with a single file stored along with its metadata and reference files.
# This uses a store depth of 3, with a width of 2 and "SHA-256" as its default store algorithm
## Notes:
## - Objects are stored using their content identifier as the file address
## - The reference file for each pid contains a single cid
## - The reference file for each cid contains multiple pids each on its own line
## - There are two metadata docs under the metadata directory for the pid (sysmeta, annotations)

.../metacat/hashstore
├── hashstore.yaml
└── objects
|   └── 4d
|       └── 19
|           └── 81
|               └── 71eef969d553d4c9537b1811a7b078f9a3804fc978a761bc014c05972c
└── metadata
|   └── 0d
|       └── 55
|           └── 55
|               └── 5ed77052d7e166017f779cbc193357c3a5006ee8b8457230bcf7abcef65e
|                   └── 323e0799524cec4c7e14d31289cefd884b563b5c052f154a066de5ec1e477da7
|                   └── sha256(pid+formatId_annotations)
└── refs
    ├── cids
    |   └── 4d
    |       └── 19
    |           └── 81
    |               └── 71eef969d553d4c9537b1811a7b078f9a3804fc978a761bc014c05972c
    └── pids
        └── 0d
            └── 55
                └── 55
                    └── 5ed77052d7e166017f779cbc193357c3a5006ee8b8457230bcf7abcef65e
```


## Development build

HashStore is a Java package, and built using the [Maven](https://maven.apache.org/) build tool.

To install `HashStore-java` locally, install Java and Maven on your local machine,
and then install or build the package with `mvn install` or `mvn package`, respectively.

We also maintain a parallel [Python-based version of HashStore](https://github.com/DataONEorg/hashstore).

## HashStore HashStoreClient Usage

```sh

# Step 1: Get HashStore Jar file
$ mvn clean package -Dmaven.test.skip=true

# Get help
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -h

# Step 2: Determine where your hashstore should live (ex. `/var/hashstore`)
## Create a HashStore (long option)
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient --createhashstore --storepath=/path/to/store --storedepth=3 --storewidth=2 --storealgo=SHA-256 --storenamespace=http://ns.dataone.org/service/types/v2.0

## Create a HashStore (short option)
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -chs -store /path/to/store -dp 3 -wp 2 -ap SHA-256 -nsp http://ns.dataone.org/service/types/v2

# Get the checksum of a data object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -getchecksum -pid testpid1 -algo SHA-256

# Find an object in HashStore (returns its content identifier if it exists)
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -findobject -pid testpid1

# Store a data object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -storeobject -path /path/to/data.ext -pid testpid1

# Store a metadata object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -storemetadata -path /path/to/metadata.ext -pid testpid1 -format_id http://ns.dataone.org/service/types/v2

# Retrieve a data object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -retrieveobject -pid testpid1

# Retrieve a metadata object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -retrievemetadata -pid testpid1 -format_id http://ns.dataone.org/service/types/v2

# Delete a data object
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -deleteobject -pid testpid1

# Delete a metadata file
$ java -cp ./target/hashstore-1.0-SNAPSHOT.jar org.dataone.hashstore.HashStoreClient -store /path/to/store -deletemetadata -pid testpid1 -format_id http://ns.dataone.org/service/types/v2
```

## License

```txt
Copyright [2023] [Regents of the University of California]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

## Acknowledgements

Work on this package was supported by:

- DataONE Network
- Arctic Data Center: NSF-PLR grant #2042102 to M. B. Jones,  A. Budden, M. Schildhauer, and  J. Dozier

Additional support was provided for collaboration by the National Center for Ecological Analysis and Synthesis, a Center funded by the University of California, Santa Barbara, and the State of California.

[![DataONE_footer](https://user-images.githubusercontent.com/6643222/162324180-b5cf0f5f-ae7a-4ca6-87c3-9733a2590634.png)](https://dataone.org)

[![nceas_footer](https://www.nceas.ucsb.edu/sites/default/files/2020-03/NCEAS-full%20logo-4C.png)](https://www.nceas.ucsb.edu)

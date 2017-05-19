# tstreams-transaction-server
Implements Transaction storage server for T-Streams (hereinafter - TTS)

## Table of contents

- [Launching](#launching)
    - [Java](#java)
    - [Docker](#docker)
- [License](#license)

## Launching

There is two ways to launch TTS:
- via _java_ command
- via _docker_ image

You should pass a file with properties in both cases. The file should contain the following properties:

|NAME                              |DESCRIPTION    |TYPE           |EXAMPLE        |VALID VALUES|
| -------------                    | ------------- | ------------- | ------------- | ------------- |
| host                             | ipv4 or ipv6 listen address. |string | 127.0.0.1| |
| port                             | A port.  |int    |8071| |
| key                              | The key to authorize.  |string |key| |
| active.tokens.number             | The number of active tokens a server can handle over time.  |int    |100| [1,...]|
| token.ttl                        | The time a token live before expiration.  |int    | 120| [1,...]|
| path                             | The path where folders of Commit log, berkeley environment and rocksdb databases would be placed.  |string |/tmp| |
| data.directory                   | The path where rocksdb databases are placed relatively to property "path".  |string |transaction_data| |
| metadata.directory               | The path where a berkeley environment and it's databases are placed relatively to "path".  |string |transaction_metadata| |
| commit.log.directory             | the path where commit log files are placed relatively to "path".  |string |commmit_log| |
| commit.log.rocks.directory       | the path where rocksdb with persisted commit log files is placed relatively to "path".  |string |commit_log_rocks| |  
| auth.key                         | The special security token which is used by the slaves to authenticate on master.| string | server_group| |
| endpoints                        | ???  |string | 127.0.0.1:8071 | |
| name                             | ???  |string |server| |  
| group                            | ???  |string |group| |  
| write.thread.pool                | The number of threads of pool are used to do write operations from Rocksdb databases. Used for: putTransactionData. |int    | 4| [1,...]|    
| read.thread.pool                 | The number of threads of pool are used to do read operations from Rocksdb databases. Used for: getTransactionData.  |int    | 2| [1,...]|
| ttl.add.ms                       | The time to add to a stream that is used to, with stream ttl, to determine how long all producer transactions data belonging to the stream live. |int    | 50| [1,...]|    
| create.if.missing                | If true, the rocksDB databases will be created if it is missing.  |boolean| true| |    
| max.background.compactions       | Is the maximum number of concurrent background compactions. The default is 1, but to fully utilize your CPU and storage you might want to increase this to approximately number of cores in the system.  |int    | 1| [1,...]|    
| allow.os.buffer                  | If false, we will not buffer files in OS cache. Look at: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide  |boolean| true| | 
| compression                      | Compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION]. If it's unimportant use a *LZ4_COMPRESSION* as default value.  |string |LZ4_COMPRESSION| | 
| use.fsync                        | If true, then every store to stable storage will issue a fsync. If false, then every store to stable storage will issue a fdatasync. This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.   |boolean| true| |  
| zk.endpoints                     | The socket address(es) of ZooKeeper servers.  |string |127.0.0.1:2181| |  
| zk.prefix                        | The coordination path to get/put socket address of t-streams transaction server.  |string |/tts | |
| zk.session.timeout.ms            | The time to wait while trying to re-establish a connection to a ZooKeepers server(s).  |int    | 10000| [1,...]|     
| zk.retry.delay.ms                | Delays between retry attempts to establish connection to ZooKeepers server on case of lost connection.  |int    | 500| [1,...]|    
| zk.connection.timeout.ms         | The time to wait while trying to establish a connection to a ZooKeepers server(s) on first connection.  |int    | 10000| [1,...]|
| max.metadata.package.size        | The size of metadata package that client can transmit or request to/from server, i.e. calling 'scanTransactions' method. If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception. If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client. If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset. |int    | 10000| [1,...]|
| max.data.package.size            | The size of data package that client can transmit or request to/from server, i.e. calling 'getTransactionData' method. If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception. If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client. If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset. |int    | 10000| [1,...]|
| commit.log.write.sync.policy     | Policies to work with commitlog. If 'every-n-seconds' mode is chosen then data is flushed into file when specified count of seconds from last flush operation passed. If 'every-new-file' mode is chosen then data is flushed into file when new file starts. If 'every-nth' mode is chosen then data is flushed into file when specified count of write operations passed.  |string    | every-nth| [every-n-seconds, every-nth, every-new-file]|  
| commit.log.write.sync.value      | Count of write operations or count of seconds between flush operations. It depends on the selected policy |int    | 10000| [1,...]|
| incomplete.commit.log.read.policy| Policies to read from commitlog files. If 'resync-majority' mode is chosen then ???(not implemented yet). If 'skip-log' mode is chosen commit log files than haven't md5 file are not read. If 'try-read' mode is chosen commit log files than haven't md5 file are tried to be read. If 'error' mode is chosen commit log files than haven't md5 file throw throwable and stop server working. |string |error |[resync-majority (mandatory for replicated mode), skip-log, try-read, error] |
| commit.log.close.delay.ms        | the time through a commit log file is closed. |int  |200    |
| commit.log.file.ttl.sec          | the time a commit log files live before they are deleted. | int | 86400 |
| counter.path.file.id.gen         | the coordination path for counter for generating and retrieving commit log file id. | string | /server_counter/file_id_gen |

It isn't required to adhere the specified order of the properties, it's for example only. 
But all properties should be defined with the exact names and appropriate types. 

### Java

In addition to the properties file you should provide two dependencies through adding jars of 'slf4j-api-1.7.24' 
and 'slf4j-log4j12-1.7.24' to a classpath, to launch TTS. That is run the following command:

```bash
java -Dconfig=<path_to_config>/config.properties -cp <path_to_TTS_jar>/tstreams-transaction-server-<version>.jar:<path_to_slf4j_api_jar>/slf4j-api-1.7.24.jar:<path_to_slf4j_impl_jar>/slf4j-log4j12-1.7.24.jar com.bwsw.tstreamstransactionserver.ServerLauncher
```

### Docker

The docker file is in the root directory. To build image: 

```bash
docker build --tag bwsw/tstreams-transaction-server .
```

To download image use:
```bash
docker pull bwsw/tstreams-transaction-server
```

To run docker image you should provide a path to config directory where a file named 'config.properties' is, specify the external host and port to be able to connect:

```bash
docker run -v <path_to_conf_directory>:/etc/conf -p <external_port>:<port> -e HOST=<external_host> -e PORT0=<external_port> bwsw/tstreams-transaction-server
```

## License

Released under Apache 2.0 License

# tstreams-transaction-server
Implements Transaction storage server for T-Streams (hereinafter - TTS)

## Table of contents

- [Launching](#launching)
    - [Java](#java)
    - [Docker](#docker)

## Launching

There is two ways to launch TTS:
- via _java_ command
- via _docker_ image

You should pass a file with properties in both cases. The file should contain the following properties:

|NAME                             |DESCRIPTION    |TYPE           |EXAMPLE        |VALID VALUES|
| -------------                   | ------------- | ------------- | ------------- | ------------- |
| host                            |   |string | 127.0.0.1| |
| port                            |   |int    |8071| |
| thread.pool                     |   |int    | 4| [1,...]|
| key                             |   |string |key| |
| active.tokens.number            |   |int    |100| [1,...]|
| token.ttl                       |   |int    | 120| [1,...]|
| path                            |   |string |/tmp| |
| clear.delay.ms                  |   |int    | 10| [1,...]|
| clear.amount                    |   |int    | 200| [1,...]|
| stream.directory                |   |string |stream| |
| consumer.directory              |   |string |consumer| |
| data.directory                  |   |string |transaction_data| |
| metadata.directory              |   |string |transaction_metadata| |
| stream.storage.name             |   |string |StreamStore| |  
| consumer.storage.name           |   |string |ConsumerStore| |   
| metadata.storage.name           |   |string |TransactionStore| |  
| opened.transactions.storage.name|   |string |TransactionOpenStore| |  
| berkeley.read.thread.pool       |   |int    | 2| [1,...]|  
| endpoints                       |   |string | 127.0.0.1:8071 | |
| name                            |   |string |server| |  
| group                           |   |string |group| |  
| write.thread.pool               |   |int    | 4| [1,...]|    
| read.thread.pool                |   |int    | 2| [1,...]|
| ttl.add.ms                      |   |int    | 50| [1,...]|    
| create.if.missing               |   |boolean| true| |    
| max.background.compactions      |   |int    | 1| [1,...]|    
| allow.os.buffer                 |   |boolean| true| | 
| compression                     | Compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION]. If it's unimportant use a *LZ4_COMPRESSION* as default value.  |string |LZ4_COMPRESSION| | 
| use.fsync                       |   |boolean| true| |  
| zk.endpoints                    |   |string |127.0.0.1:2181| |  
| zk.prefix                       |   |string |/tts | |
| zk.session.timeout.ms           |   |int    | 10000| [1,...]|     
| zk.retry.delay.ms               |   |int    | 500| [1,...]|    
| zk.connection.timeout.ms        |   |int    | 10000| [1,...]|

It isn't required to adhere the specified order of the properties, it's for example only. 
But all properties should be defined with the exact names and appropriate types. 

### Java

In addition to the properties file you should provide two dependencies through adding jars of 'slf4j-api-1.7.21' 
and 'slf4j-simple-1.7.21' to a classpath, to launch TTS. That is run the following command:

```bash
java -Dconfig=<path_to_config>/config.properties -cp <path_to_TTS_jar>/tstreams-transaction-server-<version>.jar:<path_to_slf4j_api_jar>/slf4j-api-1.7.21.jar:<path_to_slf4j_impl_jar>/slf4j-simple-1.7.21.jar com.bwsw.tstreamstransactionserver.ServerLauncher
```

### Docker


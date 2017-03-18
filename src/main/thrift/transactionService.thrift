namespace scala transactionService.rpc

enum TransactionStates {
    Opened       = 1
    Updated      = 2
    Cancel       = 3
    Invalid      = 4
    Checkpointed = 5
}

typedef string StreamType
typedef i32    PartitionType
typedef i64    transactionIDType
typedef i32    tokenType
typedef i64    tllType

struct ProducerTransaction {
   1: required StreamType          stream
   2: required PartitionType       partition
   3: required transactionIDType   transactionID
   4: required TransactionStates   state
   5: required i32                 quantity
   6: required tllType             ttl
}

struct ConsumerTransaction {
   1: required StreamType          stream
   2: required PartitionType       partition
   3: required transactionIDType   transactionID
   4: required string              name
}

struct Transaction {
    1: optional ProducerTransaction    producerTransaction
    2: optional ConsumerTransaction    consumerTransaction
}

struct Stream {
    1: required StreamType    name
    2: required i32           partitions
    3: optional string        description
    4: required tllType       ttl
}

struct AuthInfo {
    1: required tokenType     token
    2: required i32           maxMetadataPackageSize
    3: required i32           maxDataPackageSize
}

struct ResponseScanTransactions {
    1: required list<ProducerTransaction>   producerTransactions
    2: required bool                        isResponseCompleted
}

exception ServerException {
    1: string message;
}



service StreamService {

  bool putStream(1: StreamType stream, 2: i32 partitions, 3: optional string description, 4: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: StreamType stream) throws (1:ServerException error),

  Stream getStream(1: StreamType stream) throws (1:ServerException error),

  bool delStream(1: StreamType stream) throws (1:ServerException error)
}



service TransactionMetaService {

   bool putTransaction(1: Transaction transaction) throws (1:ServerException error),

   bool putTransactions(1: list<Transaction> transactions) throws (1:ServerException error),

   ResponseScanTransactions scanTransactions(1: StreamType stream, 2: PartitionType partition, 4: i64 from, 5: i64 to) throws (1:ServerException error),

}



service TransactionDataService {

  bool putTransactionData(1: StreamType stream, 2: PartitionType partition, 3: transactionIDType transaction, 4: list<binary> data, 5: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: StreamType stream, 2: PartitionType partition, 3: transactionIDType transaction, 4: i32 from, 5: i32 to) throws (1:ServerException error)
}


service ConsumerService {

 bool putConsumerCheckpoint(1: string name, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction) throws (1:ServerException error),

 i64 getConsumerState(1: string name, 2: StreamType stream, 3: PartitionType partition) throws (1:ServerException error)
}


service authService {

  AuthInfo authenticate(1: string authKey),

  bool isValid(1: tokenType token)
}


service TransactionService {

  bool putStream(1: StreamType stream, 2: i32 partitions, 3: optional string description, 4: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: StreamType stream) throws (1:ServerException error),

  Stream getStream(1: StreamType stream) throws (1:ServerException error),

  bool delStream(1: StreamType stream) throws (1:ServerException error),

  bool putTransaction(1: Transaction transaction) throws (1:ServerException error),

  bool putTransactions(1: list<Transaction> transactions) throws (1:ServerException error),

  ResponseScanTransactions scanTransactions(1: StreamType stream, 2: PartitionType partition, 3: i64 from, 4: i64 to) throws (1:ServerException error),

  bool putTransactionData(1: StreamType stream, 2: PartitionType partition, 3: transactionIDType transaction, 4: list<binary> data, 5: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: StreamType stream, 2: PartitionType partition, 3: transactionIDType transaction, 4: i32 from, 5: i32 to) throws (1:ServerException error),

  bool putConsumerCheckpoint(1: string name, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction) throws (1:ServerException error),

  transactionIDType getConsumerState(1: string name, 2: StreamType stream, 3: PartitionType partition) throws (1:ServerException error),

  AuthInfo authenticate(1: string authKey),

  bool isValid(1: tokenType token)
}

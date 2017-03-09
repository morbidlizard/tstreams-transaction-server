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

exception ServerException {
    1: string message;
}



service StreamService {

  bool putStream(1: tokenType token, 2: StreamType stream, 3: i32 partitions, 4: optional string description, 5: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: tokenType token, 2: StreamType stream) throws (1:ServerException error),

  Stream getStream(1: tokenType token, 2: StreamType stream) throws (1:ServerException error),

  bool delStream(1: tokenType token, 2: StreamType stream) throws (1:ServerException error)
}



service TransactionMetaService {

   bool putTransaction(1: tokenType token, 2: Transaction transaction) throws (1:ServerException error),

   bool putTransactions(1: tokenType token, 2: list<Transaction> transactions) throws (1:ServerException error),

   list<Transaction> scanTransactions(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: i64 from, 5: i64 to) throws (1:ServerException error),

}



service TransactionDataService {

  bool putTransactionData(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: list<binary> data, 6: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: i32 from, 6: i32 to) throws (1:ServerException error)
}


service ConsumerService {

 bool setConsumerState(1: tokenType token, 2: string name, 3: StreamType stream, 4: PartitionType partition, 5: transactionIDType transaction) throws (1:ServerException error),

 i64 getConsumerState(1: tokenType token, 2: string name, 3: StreamType stream, 4: PartitionType partition) throws (1:ServerException error)
}


service authService {

  tokenType authenticate(1: string authKey),

  bool isValid(1: tokenType token)
}


service TransactionService {

  bool putStream(1: tokenType token, 2: StreamType stream, 3: i32 partitions, 4: optional string description, 5: tllType ttl) throws (1:ServerException error),

  bool checkStreamExists(1: tokenType token, 2: StreamType stream) throws (1:ServerException error),

  Stream getStream(1: tokenType token, 2: StreamType stream) throws (1:ServerException error),

  bool delStream(1: tokenType token, 2: StreamType stream) throws (1:ServerException error),

  bool putTransaction(1: tokenType token, 2: Transaction transaction) throws (1:ServerException error),

  bool putTransactions(1: tokenType token, 2: list<Transaction> transactions) throws (1:ServerException error),

  list<Transaction> scanTransactions(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: i64 from, 5: i64 to) throws (1:ServerException error),

  bool putTransactionData(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: list<binary> data, 6: i32 from) throws (1:ServerException error),

  list <binary> getTransactionData(1: tokenType token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: i32 from, 6: i32 to) throws (1:ServerException error),

  bool setConsumerState(1: tokenType token, 2: string name, 3: StreamType stream, 4: PartitionType partition, 5: transactionIDType transaction) throws (1:ServerException error),

  transactionIDType getConsumerState(1: tokenType token, 2: string name, 3: StreamType stream, 4: PartitionType partition) throws (1:ServerException error),

  tokenType authenticate(1: string authKey),

  bool isValid(1: tokenType token)
}

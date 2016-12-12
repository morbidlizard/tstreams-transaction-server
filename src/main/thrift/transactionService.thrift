namespace scala transactionService.rpc

enum TransactionStates {
    Opened       = 1
    Checkpointed = 2
    Invalid      = 3
}

typedef string StreamType
typedef i32    PartitionType
typedef i64    transactionIDType

struct ProducerTransaction {
   1: required StreamType          stream
   2: required PartitionType       partition
   3: required transactionIDType   transactionID
   4: required TransactionStates   state
   5: required i32                 quantity
   6: required i64                 timestamp
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
    4: required i32           ttl
}



service StreamService {

  bool putStream(1: string token, 2: StreamType stream, 3: i32 partitions, 4: optional string description, 5: i32 ttl),

  bool doesStreamExist(1: string token, 2: StreamType stream),

  Stream getStream(1: string token, 2: StreamType stream),

  bool delStream(1: string token, 2: StreamType stream)
}



service TransactionMetaService {

   bool putTransaction(1: string token, 2: Transaction transaction),

   bool putTransactions(1: string token, 2: list<Transaction> transactions),

   list<Transaction> scanTransactions(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: i64 from, 5: i64 to),

}



service TransactionDataService {

  bool putTransactionData(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: list<binary> data, 6: i32 from),

  list <binary> getTransactionData(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: i32 from, 6: i32 to)
}


service ConsumerService {

 bool setConsumerState(1: string token, 2: string name, 3: StreamType stream, 4: PartitionType partition, 5: transactionIDType transaction),

 i64 getConsumerState(1: string token, 2: string name, 3: StreamType stream, 4: PartitionType partition)
}


service TransactionService {

  bool putStream(1: string token, 2: StreamType stream, 3: i32 partitions, 4: optional string description, 5: i32 ttl),

  bool doesStreamExist(1: string token, 2: StreamType stream),

  Stream getStream(1: string token, 2: StreamType stream),

  bool delStream(1: string token, 2: StreamType stream),

  bool putTransaction(1: string token, 2: Transaction transaction),

  bool putTransactions(1: string token, 2: list<Transaction> transactions),

  list<Transaction> scanTransactions(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: i64 from, 5: i64 to),

  bool putTransactionData(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: list<binary> data, 6: i32 from),

  list <binary> getTransactionData(1: string token, 2: StreamType stream, 3: PartitionType partition, 4: transactionIDType transaction, 5: i32 from, 6: i32 to),

  bool setConsumerState(1: string token, 2: string name, 3: StreamType stream, 4: PartitionType partition, 5: transactionIDType transaction),

  i64 getConsumerState(1: string token, 2: string name, 3: StreamType stream, 4: PartitionType partition)
}

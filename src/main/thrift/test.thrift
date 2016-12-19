namespace scala test

enum States {
    Opened       = 1
    Updated      = 2
    Invalid      = 3
    Checkpointed = 4
}

struct ProducerTxn {
   1: required string          stream
   2: required i32       partition
   3: required i64   transactionID
   4: required States   state
   5: required i32                 quantity
   6: required i64                 keepAliveTTL
}

struct ConsumerTxn {
   1: required string          stream
   2: required i32       partition
   3: required i64   transactionID
   4: required string              name
}

struct Txn {
    1: optional ProducerTxn    producerTransaction
    2: optional ConsumerTxn    consumerTransaction
}
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerServiceImpl
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler.LastTransactionStreamPartition

abstract class TransactionMetaService(rocksDB: KeyValueDatabaseManager,
                                      lastTransactionStreamPartition: LastTransactionStreamPartition,
                                      consumerService: ConsumerServiceImpl)
{

}

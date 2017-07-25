package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerStateMachineCache, ProducerTransactionRecord, TransactionMetaServiceImpl}
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

class TestTransactionMetaServiceImpl(rocksDB: KeyValueDatabaseManager,
                                     producerStateMachine: ProducerStateMachineCache,
                                     notifier: StateNotifier[ProducerTransaction])
  extends TransactionMetaServiceImpl(rocksDB, producerStateMachine)
{
  override protected def onProducerTransactionStateChangeDo: (ProducerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerStateMachineCache, ProducerTransactionRecord, TransactionMetaServiceWriter}
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

class TestTransactionMetaServiceWriter(rocksDB: KeyValueDbManager,
                                       producerStateMachine: ProducerStateMachineCache,
                                       notifier: StateNotifier[ProducerTransaction])
  extends TransactionMetaServiceWriter(rocksDB, producerStateMachine)
{
  override protected def onProducerTransactionStateChangeDo: (ProducerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

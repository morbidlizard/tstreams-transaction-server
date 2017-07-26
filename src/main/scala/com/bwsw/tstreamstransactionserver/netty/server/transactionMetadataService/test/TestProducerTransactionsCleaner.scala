package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionsCleaner, ProducerTransactionRecord}
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

class TestProducerTransactionsCleaner(rocksDB: KeyValueDatabaseManager,
                                      notifier: StateNotifier[ProducerTransaction])
  extends ProducerTransactionsCleaner(rocksDB)
{
  override protected def onProducerTransactionStateChangeDo: (ProducerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{Cleaner, ProducerTransactionRecord}
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction

class TestCleaner(rocksDB: KeyValueDatabaseManager,
                  notifier: StateNotifier[ProducerTransaction])
  extends Cleaner(rocksDB)
{
  override protected def onProducerTransactionStateChangeDo: (ProducerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

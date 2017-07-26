package com.bwsw.tstreamstransactionserver.netty.server.consumerService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceImpl, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

class TestConsumerServiceImpl(rocksMetaServiceDB: KeyValueDatabaseManager,
                              notifier: StateNotifier[ConsumerTransaction])
  extends ConsumerServiceImpl(rocksMetaServiceDB) {

  override protected def onConsumerTransactionStateChangeDo: (ConsumerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

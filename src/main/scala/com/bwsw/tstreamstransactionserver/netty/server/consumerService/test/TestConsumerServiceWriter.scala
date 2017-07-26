package com.bwsw.tstreamstransactionserver.netty.server.consumerService.test

import com.bwsw.tstreamstransactionserver.netty.server.StateNotifier
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerServiceWriter, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction

class TestConsumerServiceWriter(rocksMetaServiceDB: KeyValueDbManager,
                                notifier: StateNotifier[ConsumerTransaction])
  extends ConsumerServiceWriter(rocksMetaServiceDB) {

  override protected def onConsumerTransactionStateChangeDo: (ConsumerTransactionRecord) => Unit = {
    transaction => {
      notifier.tryCompleteRequests(transaction)
    }
  }
}

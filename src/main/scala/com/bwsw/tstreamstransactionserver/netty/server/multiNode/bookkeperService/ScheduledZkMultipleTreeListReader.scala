package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.TransactionServer
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabaseManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordType}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.LedgerIDAndItsLastRecordID
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ScheduledZkMultipleTreeListReader(zkMultipleTreeListReader: ZkMultipleTreeListReader,
                                        transactionServer: TransactionServer,
                                        rocksDBStorage: KeyValueDatabaseManager)
{
  private type Timestamp = Long

  val (records, ledgerIDsAndTheirLastRecordIDs) =
    zkMultipleTreeListReader.process(Array.empty[LedgerIDAndItsLastRecordID])

//  def test(array: Array[Record]) = {
//     val recordsByType = array.groupBy(record => record.recordType)
//
//     val priorityQueue = new mutable.PriorityQueue[]()
//
//     recordsByType.get(RecordType.ProducerTransaction)
//      .map(records =>
//        val transactions = ArrayBuffer.empty[(Transaction, Timestamp)]
//        transactions.
//
//
//      {
//        records.foldLeft((
//
//        )) { case (producerTransactions) }
//      })
//
//     recordsByType.get(RecordType.ConsumerTransaction)
//  }

}

package com.bwsw.tstreamstransactionserver.netty.server

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord

trait Commitable {
  def putProducerTransactions(producerTransactions: Seq[ProducerTransactionRecord])

  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord])

  def putProducerData(streamID: Int,
                      partition: Int,
                      transaction: Long,
                      data: Seq[ByteBuffer],
                      from: Int): Boolean

  def commit(): Boolean
}

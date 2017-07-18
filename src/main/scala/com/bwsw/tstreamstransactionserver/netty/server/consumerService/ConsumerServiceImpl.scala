/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.consumerService

import com.bwsw.tstreamstransactionserver.netty.server.RocksStorage
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{Batch, RocksDBALL}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


class ConsumerServiceImpl(rocksMetaServiceDB: RocksDBALL)
  extends ConsumerTransactionStateNotifier {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val consumerDatabase = rocksMetaServiceDB.getDatabase(RocksStorage.CONSUMER_STORE)

  def getConsumerState(name: String, streamID: Int, partition: Int): Long = {
    val consumerTransactionKey = ConsumerTransactionKey(name, streamID, partition).toByteArray
    val consumerTransactionValue = Option(consumerDatabase.get(consumerTransactionKey))
    consumerTransactionValue.map(bytes =>
      ConsumerTransactionValue.fromByteArray(bytes).transactionId
    ).getOrElse {
      if (logger.isDebugEnabled()) logger.debug(s"There is no checkpointed consumer transaction on stream $name, partition $partition with name: $name. Returning -1L")
      -1L
    }
  }

  private final def transitConsumerTransactionToNewState(commitLogTransactions: Seq[ConsumerTransactionRecord]): ConsumerTransactionRecord = {
    commitLogTransactions.maxBy(_.timestamp)
  }

  private final def groupProducerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]) = {
    consumerTransactions.groupBy(txn => txn.key)
  }

  def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord], batch: Batch): ListBuffer[Unit => Unit] = {
    if (logger.isDebugEnabled()) logger.debug(s"Trying to commit consumer transactions: $consumerTransactions")
    val notifications = new ListBuffer[Unit => Unit]()
    groupProducerTransactions(consumerTransactions) foreach { case (key, txns) =>
      val theLastStateTransaction = transitConsumerTransactionToNewState(txns)
      val consumerTransactionValueBinary = theLastStateTransaction.consumerTransaction.toByteArray
      val consumerTransactionKeyBinary = key.toByteArray
      batch.put(RocksStorage.CONSUMER_STORE, consumerTransactionKeyBinary, consumerTransactionValueBinary)
      if (areThereAnyConsumerNotifies) notifications += tryCompleteConsumerNotify(theLastStateTransaction)
    }
    notifications
  }
}
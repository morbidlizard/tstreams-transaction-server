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

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDbBatch, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage
import org.slf4j.LoggerFactory


class ConsumerServiceWriter(rocksMetaServiceDB: KeyValueDbManager) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private final def getLastTransaction(commitLogTransactions: Seq[ConsumerTransactionRecord]): ConsumerTransactionRecord = {
    commitLogTransactions.maxBy(_.timestamp)
  }

  protected def onStateChange: ConsumerTransactionRecord => Unit = {
    _ => {}
  }

  def putConsumersCheckpoints(consumerTransactions: Seq[ConsumerTransactionRecord],
                              batch: KeyValueDbBatch): Unit = {
    if (logger.isDebugEnabled())
      logger.debug(s"Trying to commit consumer transactions: $consumerTransactions")

    val groupedConsumerTransactions =
      consumerTransactions.groupBy(txn => txn.key)

    groupedConsumerTransactions.foreach {
      case (key, txns) =>
        val lastTransaction = getLastTransaction(txns)
        val value = lastTransaction.consumerTransaction.toByteArray

        batch.put(
          RocksStorage.CONSUMER_STORE,
          key.toByteArray,
          value
        )

        onStateChange(lastTransaction)
    }
  }
}
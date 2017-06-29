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
package com.bwsw.tstreamstransactionserver.netty.server


import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabaseBatch, KeyValueDatabaseManager}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDBALL, RocksDatabaseDescriptor}
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}


class RocksStorage(storageOpts: StorageOptions, rocksOpts: RocksStorageOptions, readOnly: Boolean = false) {
  private val columnFamilyOptions = rocksOpts.createColumnFamilyOptions()
  val rocksMetaServiceDB: KeyValueDatabaseManager = new RocksDBALL(
    storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
    rocksOpts,
    Seq(
      RocksDatabaseDescriptor("LastOpenedTransactionStorage".getBytes(),       columnFamilyOptions),
      RocksDatabaseDescriptor("LastCheckpointedTransactionStorage".getBytes(), columnFamilyOptions),
      RocksDatabaseDescriptor("ConsumerStore".getBytes(),                      columnFamilyOptions),
      RocksDatabaseDescriptor("CommitLogStore".getBytes(),                     columnFamilyOptions),
      RocksDatabaseDescriptor("TransactionAllStore".getBytes(),
        columnFamilyOptions,
        TimeUnit.MINUTES.toSeconds(rocksOpts.transactionExpungeDelayMin).toInt
      ),
      RocksDatabaseDescriptor("TransactionOpenStore".getBytes(),               columnFamilyOptions)
    ),
    readOnly
  )
  def newBatch: KeyValueDatabaseBatch = rocksMetaServiceDB.newBatch
}

object RocksStorage {
  val LAST_OPENED_TRANSACTION_STORAGE = 1
  val LAST_CHECKPOINTED_TRANSACTION_STORAGE = 2
  val CONSUMER_STORE = 3
  val COMMIT_LOG_STORE = 4
  val TRANSACTION_ALL_STORE = 5
  val TRANSACTION_OPEN_STORE = 6
}
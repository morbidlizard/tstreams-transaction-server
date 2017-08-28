package com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService

import com.bwsw.tstreamstransactionserver.netty.server.batch.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerMetadata, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage

final class CommitLogService(rocksDB: KeyValueDbManager) {
  private val bookkeeperLogDatabase = rocksDB.getDatabase(RocksStorage.BOOKKEEPER_LOG_STORE)

  //TODO rename function
  def getLastProcessedLedgersAndRecordIDs: Array[LedgerMetadata] = {
    val iterator = bookkeeperLogDatabase.iterator
    iterator.seek(BigCommit.bookkeeperKey)

    val records = if (iterator.isValid)
      Some(MetadataRecord.fromByteArray(iterator.value()).records)
    else
      None

    iterator.close()
    records.getOrElse(Array.empty[LedgerMetadata])
  }


  def getMinMaxLedgersIds: MinMaxLedgerIDs = {
    val ledgers = getLastProcessedLedgersAndRecordIDs
    if (ledgers.isEmpty) {
      MinMaxLedgerIDs(-1L, -1L)
    }
    else {
      val min = ledgers.minBy(_.id).id
      val max = ledgers.maxBy(_.id).id
      MinMaxLedgerIDs(min, max)
    }
  }
}

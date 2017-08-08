package com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService

import com.bwsw.tstreamstransactionserver.netty.server.batch.BigCommit
import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.metadata.{LedgerIDAndItsLastRecordID, MetadataRecord}
import com.bwsw.tstreamstransactionserver.netty.server.storage.RocksStorage

final class CommitLogService(rocksDB: KeyValueDbManager) {
  private val bookkeeperLogDatabase = rocksDB.getDatabase(RocksStorage.BOOKKEEPER_LOG_STORE)

  def getLastProcessedLedgersAndRecordIDs: Array[LedgerIDAndItsLastRecordID] = {
    val iterator = bookkeeperLogDatabase.iterator
    iterator.seek(BigCommit.bookkeeperKey)

    val records = if (iterator.isValid)
      Some(MetadataRecord.fromByteArray(iterator.value()).records)
    else
      None

    iterator.close()
    records.getOrElse(Array.empty[LedgerIDAndItsLastRecordID])
  }
}

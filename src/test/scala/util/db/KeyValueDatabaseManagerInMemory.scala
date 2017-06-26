package util.db

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDatabase, KeyValueDatabaseBatch, KeyValueDatabaseManager}

class KeyValueDatabaseManagerInMemory(dbs: Array[KeyValueDatabase])
  extends KeyValueDatabaseManager {

  override def getDatabase(index: Int): KeyValueDatabase =
    dbs(index)

  override def getRecordFromDatabase(index: Int, key: Array[Byte]): Array[Byte] =
    dbs(index).get(key)

  override def newBatch: KeyValueDatabaseBatch =
    new KeyValueDatabaseBatchInMemory(dbs)

  override def close(): Unit = () //nothing
}

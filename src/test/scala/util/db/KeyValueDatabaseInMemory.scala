package util.db

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDatabase

import scala.collection.concurrent.TrieMap

class KeyValueDatabaseInMemory
  extends KeyValueDatabase {
  private val db = new TrieMap[Array[Byte], Array[Byte]]()

  override def get(key: Array[Byte]): Array[Byte] =
    db(key)

  override def put(key: Array[Byte], data: Array[Byte]): Boolean = {
    db.put(key, data)
    true
  }

  override def delete(key: Array[Byte]): Boolean = {
    Option(db.remove(key)).isDefined
  }

  override def getLastRecord: Option[(Array[Byte], Array[Byte])] = {
    db.lastOption
  }
}

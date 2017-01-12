package netty.server.transactionDataService

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import scala.concurrent.{Future => ScalaFuture}
import netty.server.{Authenticable, CheckpointTTL}
import `implicit`.Implicits._
import transactionService.rpc.TransactionDataService
import netty.server.db.rocks.RocksDbConnection

import scala.collection.mutable.ArrayBuffer

trait TransactionDataServiceImpl extends TransactionDataService[ScalaFuture]
  with Authenticable
  with CheckpointTTL {

  val ttlToAdd: Int
  private def calculateTTL(ttl: Int): Int = {
    def convertTTL = {
      val ttlToConvert = TimeUnit.MILLISECONDS.toSeconds(ttlToAdd).toInt
      if (ttlToConvert == 0) 0 else ttlToConvert
    }
    TimeUnit.HOURS.toSeconds(ttl).toInt + convertTTL
  }

  private val rocksDBStorageToStream = new java.util.concurrent.ConcurrentHashMap[StorageName, RocksDbConnection]()
  private def getStorage(stream: String, partition: Int, ttl: Int) = {
    val key = StorageName(stream)
    rocksDBStorageToStream.computeIfAbsent(key, new java.util.function.Function[StorageName, RocksDbConnection]{
      override def apply(t: StorageName): RocksDbConnection = new RocksDbConnection(key.toString, calculateTTL(ttl))
    })
  }

  override def putTransactionData(token: Int, stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer], from: Int): ScalaFuture[Boolean] =
    authenticate(token) {
      val streamObj = getStreamDatabaseObject(stream)
      val rocksDB = getStorage(stream, partition, streamObj.stream.ttl)
      val batch = rocksDB.newBatch

      val rangeDataToSave = from until (from + data.length)
      val keys = rangeDataToSave map (seqId => KeyDataSeq(Key(partition, transaction), seqId).toBinary)
      (keys zip data) foreach { case (key, datum) =>
        val sizeOfSlicedData = datum.limit() - datum.position()
        val bytes = new Array[Byte](sizeOfSlicedData)
        datum.get(bytes)
        batch.put(key, bytes)
      }
      val result = batch.write()

      result
    }(netty.Context.rocksWritePool.getContext)


  override def getTransactionData(token: Int, stream: String, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[ByteBuffer]] =
    authenticate(token) {
      val streamObj = getStreamDatabaseObject(stream)
      val rocksDB = getStorage(stream, partition, streamObj.stream.ttl)

      val fromSeqId = KeyDataSeq(Key(partition, transaction), from).toBinary
      val toSeqId = KeyDataSeq(Key(partition, transaction), to).toBinary

      val iterator = rocksDB.iterator
      iterator.seek(fromSeqId)

      val data = new ArrayBuffer[ByteBuffer](to - from)
      while (iterator.isValid && ByteArray.compare(iterator.key(), toSeqId) <= 0) {
        data += java.nio.ByteBuffer.wrap(iterator.value())
        iterator.next()
      }
      iterator.close()
      data
    }(netty.Context.rocksReadPool.getContext)

}

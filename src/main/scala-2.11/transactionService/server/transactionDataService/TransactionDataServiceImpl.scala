package transactionService.server.transactionDataService

import java.nio.ByteBuffer
import java.util.concurrent.TimeUnit

import com.twitter.util.{Future => TwitterFuture}
import transactionService.server.{Authenticable, CheckpointTTL}
import transactionService.server.db.RocksDbConnection
import transactionService.server.`implicit`.Implicits._
import transactionService.rpc.TransactionDataService

import scala.collection.mutable.ArrayBuffer

trait TransactionDataServiceImpl extends TransactionDataService[TwitterFuture]
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
    val key = StorageName(stream, partition)
    val client = new RocksDbConnection(key.toString, calculateTTL(ttl))
    Option(rocksDBStorageToStream.putIfAbsent(key, client)) match {
      case Some(existingClient) => existingClient
      case None => client
    }
  }


  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer]): TwitterFuture[Boolean] =
    authenticateFutureBody(token) {
      getStreamTTL(stream).flatMap { ttl =>
        TwitterFuture {
          val rocksDB = getStorage(stream, partition, ttl)
          val keyToStartWrite = Key(stream, partition, transaction).maxDataSeq
          val indexOfKeyToWrite = Option(rocksDB.get(keyToStartWrite))
          val delta = indexOfKeyToWrite match {
            case Some(bytes) => java.nio.ByteBuffer.wrap(bytes).getInt(0)
            case None => 0
          }

          val batch = rocksDB.newBatch

          if (indexOfKeyToWrite.isDefined) batch.remove(keyToStartWrite)
          batch.put(keyToStartWrite, delta + data.length)

          val rangeDataToSave = delta until (delta + data.length)
          val keys = rangeDataToSave map (seqId => KeyDataSeq(Key(stream, partition, transaction), seqId).toString)
          (keys zip data) foreach { case (key, datum) =>
            val sizeOfSlicedData = datum.limit() - datum.position()
            val bytes = new Array[Byte](sizeOfSlicedData)
            datum.get(bytes)
            batch.put(key, bytes)
          }

          val result = batch.write()
          result
        }
      }
    }

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] =
    authenticateFutureBody(token) {
      getStreamTTL(stream).flatMap { ttl =>
        TwitterFuture {
          val rocksDB = getStorage(stream, partition, ttl)

          val fromSeqId = KeyDataSeq(Key(stream, partition, transaction), from).toString
          val toSeqId = KeyDataSeq(Key(stream, partition, transaction), to).toString

          val iterator = rocksDB.iterator
          iterator.seek(fromSeqId)


          val data = new ArrayBuffer[ByteBuffer](to - from)
          while (iterator.isValid && new String(iterator.key()) <= toSeqId) {
            data += java.nio.ByteBuffer.wrap(iterator.value())
            iterator.next()
          }
          iterator.close()
          data
        }
      }
    }
}

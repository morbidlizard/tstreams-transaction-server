package transactionService.server.transactionDataService

import java.nio.ByteBuffer

import com.twitter.util.{Future => TwitterFuture}
import org.apache.commons.lang.StringUtils
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}
import transactionService.server.Authenticable
import transactionService.server.db.RocksDbConnection
import transactionService.server.`implicit`.Implicits._
import transactionService.rpc.TransactionDataService
import transactionService.exception.Throwables._

import scala.collection.mutable.ArrayBuffer

trait TransactionDataServiceImpl extends TransactionDataService[TwitterFuture]
  with Authenticable {

  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, data: Seq[ByteBuffer]): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        RocksDB.loadLibrary()
        val rocksDB = new RocksDbConnection()
        val client = rocksDB.client

        val keyToStartWrite = Key(stream, partition, transaction).maxDataSeq
        val indexOfKeyToWrite = Option(client.get(keyToStartWrite))
        val delta = indexOfKeyToWrite match {
          case Some(bytes) => java.nio.ByteBuffer.wrap(bytes).getInt(0)
          case None => 0
        }

        val rangeDataToSave = delta until (delta + data.length)

        val keys = rangeDataToSave map (seqId => KeyDataSeq(Key(stream, partition, transaction), seqId).toString)
        val batch = new WriteBatch()
        (keys zip data) foreach { case (key, datum) =>
          val sizeOfSlicedData = datum.limit() - datum.position()
          val bytes = new Array[Byte](sizeOfSlicedData)
          datum.get(bytes)
          batch.put(key, bytes)
        }

        if (indexOfKeyToWrite.isDefined) batch.remove(keyToStartWrite)

        batch.put(keyToStartWrite, delta + data.length)

        val result = Option(client.write(new WriteOptions(), batch)) match {
          case Some(_) => true
          case None => false
        }

        batch.close()
        rocksDB.close()

        result
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        RocksDB.loadLibrary()
        val rocksDB = new RocksDbConnection()
        val client = rocksDB.client

        val prefix = KeyDataSeq(Key(stream, partition, transaction), from).toString
        val toSeqId = KeyDataSeq(Key(stream, partition, transaction), to).toString

        val iterator = client.newIterator()
        iterator.seek(prefix)

        val data = new ArrayBuffer[ByteBuffer](to - from)
        while (iterator.isValid && new String(iterator.key()) <= toSeqId) {
          data += java.nio.ByteBuffer.wrap(iterator.value())
          iterator.next()
        }
        iterator.close()
        rocksDB.close()
        data
      }
    } else TwitterFuture.exception(tokenInvalidException)
  }
}

package transactionService.impl

import java.nio.ByteBuffer

import transactionService.impl.TransactionDataServiceImpl.{Key, KeyDataSeq}
import com.twitter.util.Future
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}
import com.twitter.util.{Future => TwitterFuture}
import transactionService.impl.db.MyRocksDbConnection
import transactionService.rpc.TransactionDataService
import transactionService.impl.`implicit`.Implicits._

import scala.collection.mutable.ArrayBuffer

trait TransactionDataServiceImpl extends TransactionDataService[Future]
  with Authenticable {
  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): TwitterFuture[Boolean] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        RocksDB.loadLibrary()
        val rangeDataToSave = from until (from + data.length)
        val keys = rangeDataToSave map (seqId => KeyDataSeq(Key(stream, partition, transaction), seqId).toString)
        val batch = new WriteBatch()
        (keys zip data) foreach { case (key, datum) =>
          val sizeOfSlicedData = datum.limit() - datum.position()
          val bytes = new Array[Byte](sizeOfSlicedData)
          datum.get(bytes)
          batch.put(key, bytes)
        }


        val rocksDB = new MyRocksDbConnection()
        val client = rocksDB.client
        val result = Option(client.write(new WriteOptions(), batch)) match {
          case Some(_) => true
          case None => false
        }

        batch.close()
        rocksDB.close()

        result
      }
    } else TwitterFuture.exception(throw new IllegalArgumentException("Token isn't valid"))
  }

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] = authClient.isValid(token) flatMap { isValid =>
    if (isValid) {
      TwitterFuture {
        RocksDB.loadLibrary()
        val prefix = KeyDataSeq(Key(stream, partition, transaction), from).toString
        val rocksDB = new MyRocksDbConnection()
        val client = rocksDB.client
        val iterator = client.newIterator()

        iterator.seek(prefix)

        def getKeyDataSeq(key: Array[Byte]) = new String(key).split("\\s+").last.toInt

        val data = new ArrayBuffer[ByteBuffer](to - from)
        while (iterator.isValid && getKeyDataSeq(iterator.key()) < to) {
          data += java.nio.ByteBuffer.wrap(iterator.value())
          iterator.next()
        }
        data += java.nio.ByteBuffer.wrap(iterator.value())

        iterator.close()
        rocksDB.close()
        data
      }
    } else TwitterFuture.exception(throw new IllegalArgumentException("Token isn't valid"))
  }
}

private object TransactionDataServiceImpl {
  case class KeyDataSeq(key: Key, dataSeq: Int) {
    override def toString: String = s"${key.toString} $dataSeq"
  }
  case class Key(stream: String, partition: Int, transaction: Long) {
    override def toString: String = s"$stream $partition $transaction"
  }
}

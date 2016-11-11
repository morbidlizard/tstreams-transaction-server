package transactionService.impl

import java.nio.ByteBuffer

import com.twitter.scrooge.TArrayByteTransport
import com.twitter.util.Future
import org.rocksdb.{RocksDB, WriteBatch, WriteOptions}
import com.twitter.util.{Future => TwitterFuture}
import org.apache.thrift.protocol.{TBinaryProtocol, TCompactProtocol}
import transactionService.impl.db.MyRocksDbConnection
import transactionService.rpc.TransactionDataService
import transactionService.impl.`implicit`.Implicits._

import scala.annotation.tailrec

trait TransactionDataServiceImpl extends TransactionDataService[Future] {
  def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, data: Seq[ByteBuffer]): TwitterFuture[Boolean] = TwitterFuture {
    RocksDB.loadLibrary()
    val rangeDataToSave = from until (from + data.length)
    val keys = rangeDataToSave map (seqId =>
      s"${TransactionDataServiceImpl.Key(stream, partition, transaction)} $seqId"
      )

    val batch = new WriteBatch()
    (keys zip data) foreach {case (key,datum) =>
      println(new String(datum.array()))
      batch.put(key, datum.array())
    }


    val rocksDB = new MyRocksDbConnection()
    val client  = rocksDB.client
    val result = Option(client.write(new WriteOptions(), batch)) match {
      case Some(_) => true
      case None => false
    }

    batch.close()
    rocksDB.close()

    result
  }

  def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): TwitterFuture[Seq[ByteBuffer]] = TwitterFuture {
    def getKeyDataSeq(key: Array[Byte]) = new String(key).split("\\s+").last.toInt
    val prefix = TransactionDataServiceImpl.Key(stream, partition, transaction).toString
    val rocksDB = new MyRocksDbConnection()
    val client  = rocksDB.client
    val iterator = client.newIterator()

    iterator.seek(prefix)

    @tailrec
    def getDataFromRange(dataSeq: Int, acc: Seq[ByteBuffer]): Seq[ByteBuffer] = {
      if (iterator.isValid && dataSeq >= to) acc
      else if (dataSeq < from) {
        iterator.next()
        getDataFromRange(getKeyDataSeq(iterator.key()), acc)
      }
      else {
        val dataDB = iterator.value()
        val data = java.nio.ByteBuffer.allocate(dataDB.length).put(dataDB)
        iterator.next()
        getDataFromRange(getKeyDataSeq(iterator.key()), acc :+ data)
      }
    }
    val result = getDataFromRange(getKeyDataSeq(iterator.key()), Seq())
    rocksDB.close()
    result
  }
}

object TransactionDataServiceImpl {
  case class Key(stream: String, partition: Int, transaction: Long) {
    override def toString: String = s"$stream $partition $transaction"
  }
}

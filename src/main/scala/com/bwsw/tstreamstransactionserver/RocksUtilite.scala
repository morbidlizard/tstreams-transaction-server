package com.bwsw.tstreamstransactionserver

import java.io.File

import com.bwsw.commitlog.filesystem.CommitLogBinary
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, FileKey, FileValue}
import org.apache.commons.io.FileUtils
import org.rocksdb._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization

import scala.collection.mutable.ArrayBuffer

object RocksUtilite {
  RocksDB.loadLibrary()

  sealed trait Transaction
  case class ProducerTransaction(stream: String, partition: Int, transactionID: Long, state: String, ttl: Long) extends Transaction
  case class ConsumerTransaction(stream: String, partition: Int, transactionID: Long, name: String) extends Transaction
  case class CommitLogRecord(id: Long, timestamp: Long, transactions: Seq[Transaction])
  case class CommitLogFile(id: Long, md5: Option[Seq[Byte]], transactions: Seq[CommitLogRecord])

  implicit val formatsTransaction = Serialization.formats(ShortTypeHints(List(classOf[Transaction])))

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) throw new IllegalArgumentException("Path to rocksdb folder should be provided.")
    else process(args(0))
  }

  def process(path: String): Unit = {
    val options: Options = new Options()
    val file = new File(path)
    val client = {
      FileUtils.forceMkdir(file)
      TtlDB.open(options, file.getAbsolutePath, -1, true)
    }

    val iterator = client.newIterator()
    iterator.seekToFirst()
    while (iterator.isValid) {
      val key = iterator.key()
      val value = iterator.value()
      val json = pretty(render(Extraction.decompose(RecordToProducerOrConsumerTransaction(key, value))))
      println(json)
      iterator.next()
    }
    iterator.close()
    client.close()
  }


  def RecordToProducerOrConsumerTransaction(key: Array[Byte], value:Array[Byte]): CommitLogFile = {
    val fileKey = FileKey.fromByteArray(key)
    val fileValue = FileValue.fromByteArray(value)
    val binaryFile = new CommitLogBinary(fileKey.id, fileValue.fileContent, fileValue.fileMD5Content)

    val recordsBuffer = new ArrayBuffer[CommitLogRecord]()
    val iterator = binaryFile.getIterator
    while (iterator.hasNext()) {
      iterator.next().right.foreach{record =>
        val transactions = CommitLogToBerkeleyWriter.retrieveTransactions(record)
        val transactionsJson = transactions.map { transaction =>
          val txn = transaction._1
          val transactionJson: Transaction = if (txn._1.isDefined) {
            txn._1.map(x => ProducerTransaction(x.stream, x.partition, x.transactionID, x.state.toString, x.ttl)).get
          } else {
            txn._2.map(x => ConsumerTransaction(x.stream, x.partition, x.transactionID, x.name)).get
          }
          transactionJson
        }
        recordsBuffer += CommitLogRecord(record.id, record.timestamp, transactionsJson)
      }
    }
    CommitLogFile(fileKey.id, fileValue.fileMD5Content.map(_.toSeq), recordsBuffer)
  }
}

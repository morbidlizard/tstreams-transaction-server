package com.bwsw.tstreamstransactionserver

import java.io.File

import com.bwsw.commitlog.CommitLogRecord
import com.bwsw.commitlog.filesystem.CommitLogBinary
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{CommitLogToBerkeleyWriter, FileKey, FileValue}
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}
import org.apache.commons.io.FileUtils
import org.rocksdb._

object RocksUtilite {
  RocksDB.loadLibrary()

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
      RecordToProducerOrConsumerTransaction(key, value)
      iterator.next()
    }
    iterator.close()
    client.close()
  }



  def producerTransactionToString(producerTransaction: ProducerTransaction, record: CommitLogRecord) = {
    import producerTransaction._
    s"record[id ${record.id}, timestamp ${record.timestamp}]: " +
      s"Producer Transaction -> stream:$stream, partition:$partition ,id:$transactionID, state:$state, ttl:$ttl"
  }

  def consumerTransactionToString(consumerTransaction: ConsumerTransaction, record: CommitLogRecord) = {
    import consumerTransaction._
    s"record[id ${record.id}, timestamp ${record.timestamp}]: " +
      s"Consumer Transaction -> stream:$stream, partition:$partition ,id:$transactionID, name:$name"
  }


  def RecordToProducerOrConsumerTransaction(key: Array[Byte], value:Array[Byte]): Unit = {
    val fileKey = FileKey.fromByteArray(key)
    val fileValue = FileValue.fromByteArray(value)
    val binaryFile = new CommitLogBinary(fileKey.id, fileValue.fileContent, fileValue.fileMD5Content)

    val iterator = binaryFile.getIterator
    while (iterator.hasNext()) {
      iterator.next().right.foreach{record =>
        val transactions = CommitLogToBerkeleyWriter.retrieveTransactions(record)
        transactions.foreach { transaction =>
          val txn = transaction._1
          if (txn._1.isDefined) {
            txn._1.foreach(x => System.out.println(producerTransactionToString(x, record)))
          } else {
            txn._2.foreach(x => System.out.println(consumerTransactionToString(x, record)))
          }
        }
      }
    }
  }
}

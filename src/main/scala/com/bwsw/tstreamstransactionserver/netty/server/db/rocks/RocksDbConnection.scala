package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.io.{Closeable, File}

import com.bwsw.tstreamstransactionserver.options.ServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.rocksdb._

class RocksDbConnection(rocksStorageOpts: RocksStorageOptions, absolutePath: String, ttl: Int = -1) extends Closeable {
  RocksDB.loadLibrary()

  private val options = rocksStorageOpts.createDBOptions()
  private val file = new File(absolutePath)
  private val client =  {
    FileUtils.forceMkdir(file)
    TtlDB.open(options, file.getAbsolutePath, ttl, false)
  }


  def get(key: Array[Byte]) = client.get(key)

  @throws[RocksDBException]
  def put(key: Array[Byte], data: Array[Byte]): Unit = client.put(key, data)


  def iterator = client.newIterator()
  override def close(): Unit = client.close()

  final def closeAndDeleteFolder(): Unit = {
    options.close()
    client.close()
    file.delete()
  }

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()
    def put(key: Array[Byte], data: Array[Byte]): Unit = {
      batch.put(key,data)
    }

    def remove(key: Array[Byte]): Unit = batch.remove(key)
    def write(): Boolean = {
      val writeOptions = new WriteOptions()
      val status = scala.util.Try(client.write(writeOptions, batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) => false
      }
      writeOptions.close()
      batch.close()
      status
    }
  }

//  def newFileWriter = new FileWriter
//  class FileWriter {
//    private val sstFileWriter = new SstFileWriter(new EnvOptions(), options, RocksDbConnection.comparator)
//    private val fileNew = new File(file.getAbsolutePath, "sst_file.sst")
//    sstFileWriter.open(fileNew.getAbsolutePath)
//
//    def putData(data: Array[Byte]): Unit = sstFileWriter.add(new Slice(data), new Slice(Array[Byte]()))
//    def finish(): Unit = {
//      sstFileWriter.finish()
//      client.compactRange()
//      client.addFileWithFilePath(fileNew.getAbsolutePath, true)
//    }
//  }
}

//private object RocksDbConnection extends App {
//  RocksDB.loadLibrary()
//  lazy val comparatorOptions = new ComparatorOptions()
//  lazy val comparator = new org.rocksdb.util.BytewiseComparator(comparatorOptions)
//}
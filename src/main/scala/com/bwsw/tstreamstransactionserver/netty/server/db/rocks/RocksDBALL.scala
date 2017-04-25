package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.io.{Closeable, File}

import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.collection.{JavaConverters, mutable}

class RocksDBALL(absolutePath: String, descriptors: Seq[RocksDatabaseDescriptor], readMode: Boolean = false) extends Closeable{
  RocksDB.loadLibrary()

  private val options = new DBOptions()
    .setCreateIfMissing(true)
    .setCreateMissingColumnFamilies(true)
  
  private[rocks] val (client, descriptorsWorkWith, databaseHandlers) = {
    val descriptorsWithDefaultDescriptor =
      (new RocksDatabaseDescriptor("default".getBytes(), new ColumnFamilyOptions()) +: descriptors).toArray

    val (columnFamilyDescriptors, ttls) = descriptorsWithDefaultDescriptor
      .map(descriptor => (new ColumnFamilyDescriptor(descriptor.name, descriptor.options), descriptor.ttl)
    ).unzip

    val databaseHandlers = new java.util.ArrayList[ColumnFamilyHandle](columnFamilyDescriptors.length)

    val file = new File(absolutePath)
    FileUtils.forceMkdir(file)

    val connection = TtlDB.open(
      options,
      file.getAbsolutePath,
      JavaConverters.seqAsJavaList(columnFamilyDescriptors),
      databaseHandlers,
      JavaConverters.seqAsJavaList(ttls),
      readMode
    )
    (connection, descriptorsWithDefaultDescriptor.toBuffer, JavaConverters.asScalaBuffer(databaseHandlers))
  }

  def getDatabasesNamesAndIndex: mutable.Seq[(Int, Array[Byte])] = descriptorsWorkWith.zipWithIndex.map{case (descriptor, index) => (index, descriptor.name)}
  def getDatabase(index: Int): RocksDBPartitionDatabase = new RocksDBPartitionDatabase(client, databaseHandlers(index))

  def getRecordFromDatabase(index: Int, key: Array[Byte]): Array[Byte] = client.get(databaseHandlers(index), key)

  def newBatch = new Batch
  class Batch() {
    private val batch  = new WriteBatch()
    def put(index: Int, key: Array[Byte], data: Array[Byte]): Unit = {
      batch.put(databaseHandlers(index), key, data)
    }

    def remove(index: Int, key: Array[Byte]): Unit = batch.remove(databaseHandlers(index), key)
    def write(): Boolean = {
      val writeOptions = new WriteOptions()
      val status = scala.util.Try(client.write(writeOptions, batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) =>
          throwable.printStackTrace()
          false
      }
      writeOptions.close()
      batch.close()
      status
    }
  }

  override def close(): Unit = client.close()
}
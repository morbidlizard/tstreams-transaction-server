package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import java.io.File

import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.collection.{JavaConverters, mutable}

class RocksDBALL(absolutePath: String, descriptors: Seq[RocksDatabaseDescriptor]) {
  RocksDB.loadLibrary()

  private val file = new File(absolutePath)

  private val (client: TtlDB, descriptorsWorkWith, databaseHandlers) = {
    val descriptorsWithDefault =
      (new RocksDatabaseDescriptor("default".getBytes(), new ColumnFamilyOptions()) +: descriptors).toArray
    val (columnFamilyDescriptors, ttls) = descriptorsWithDefault
      .map(descriptor => (new ColumnFamilyDescriptor(descriptor.name, descriptor.options), descriptor.ttl)
    ).unzip

    val databaseHandlers = new java.util.ArrayList[ColumnFamilyHandle](descriptors.length)

    FileUtils.forceMkdir(file)
    val connection = TtlDB.open(
      new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true),
      file.getAbsolutePath,
      JavaConverters.seqAsJavaList(columnFamilyDescriptors),
      databaseHandlers,
      JavaConverters.seqAsJavaList(ttls),
      false
    )
    (connection, descriptorsWithDefault, JavaConverters.asScalaBuffer(databaseHandlers))
  }

  def reopenDatabaseWithNewTTL(index: Int, ttl: Int): Unit = this.synchronized{
    val descriptor = descriptorsWorkWith(index)
    val newDescriptor = new RocksDatabaseDescriptor(descriptor.name, descriptor.options, ttl)
    descriptorsWorkWith(index) = newDescriptor
    databaseHandlers(index).close()
    databaseHandlers(index) = client.createColumnFamilyWithTtl(
      new ColumnFamilyDescriptor(newDescriptor.name, newDescriptor.options), ttl
    )
  }

  def getDatabasesNamesAndIndex: mutable.Seq[(Int, Array[Byte])] = descriptorsWorkWith.zipWithIndex.map{case (descriptor, index) => (index, descriptor.name)}
  def getDatabase(index: Int): RocksDBPartitionDatabase = new RocksDBPartitionDatabase(client, databaseHandlers(index))
}

object RocksDBALL extends App {
  val descriptor = collection.mutable.Seq(
    new RocksDatabaseDescriptor("test".getBytes(), new ColumnFamilyOptions())
  )
  val rocks = new RocksDBALL("/tmp/test", descriptor)
 // println(rocks.getDatabase(1).put("test".getBytes(), "abc".getBytes()))
  rocks.reopenDatabaseWithNewTTL(1, 100)
  println(rocks.getDatabase(1).getLastRecord)
}

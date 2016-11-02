package transactionService.impl.db

import java.io.Closeable

import org.rocksdb._
import transactionService.impl.`implicit`.Implicits._

import scala.collection.JavaConverters._


class MyRocksDbConnection(path: String = MyRocksDbConnection.path) extends Closeable{
  val (client, map) : (RocksDB, scala.collection.Map[ColumnFamilyDescriptor, ColumnFamilyHandle]) = {
    val dbOptions = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)

    val columnFamilies = RocksDB.listColumnFamilies(new Options(), path).asScala

    val descriptors = columnFamilies map (new ColumnFamilyDescriptor(_))
    val handlers = new java.util.ArrayList[ColumnFamilyHandle]().asScala

    val db = RocksDB.open(dbOptions, path, descriptors.asJava, handlers.asJava)
    val descriptorToColumnFamilyHandle = (descriptors zip handlers).toMap
    dbOptions.close()
    (db, descriptorToColumnFamilyHandle)
  }

  private def familyHandlerByDescriptorName(name: String) = {
    map.find(_._1.columnFamilyName() sameElements name) match {
      case Some(handlerToDescriptor) => Some(handlerToDescriptor._2)
      case None => None
    }
  }

  def getFamilyHandler(name: String) = {
    val handlerOpt = familyHandlerByDescriptorName(name)
    if (handlerOpt.isEmpty) {
      val descriptor = new ColumnFamilyDescriptor(name)
      val handler = client.createColumnFamily(descriptor)
      handler
    } else {
      handlerOpt.get
    }
  }

  override def close(): Unit = {
    map.keys foreach (_.columnFamilyOptions().close())
    map.values foreach (_.close())
    client.close()
  }
}

object MyRocksDbConnection {
  val path = "/tmp/rocksdb_simple_example"
}

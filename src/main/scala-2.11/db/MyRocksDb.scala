package db


import org.rocksdb._
import collection.JavaConverters._


class MyRocksDb(path: String) {

  def connection: (RocksDB, scala.collection.Map[ColumnFamilyDescriptor, ColumnFamilyHandle]) = {
    val dbOptions = new DBOptions()
      .setCreateIfMissing(true)
      .setCreateMissingColumnFamilies(true)

    val columnFamilies = RocksDB.listColumnFamilies(new Options(), path).asScala
    val descriptors = columnFamilies map (new ColumnFamilyDescriptor(_))
    val handlers = new java.util.ArrayList[ColumnFamilyHandle]().asScala

    val db = RocksDB.open(dbOptions, path, descriptors.asJava, handlers.asJava)
    val descriptorToColumnFamilyHandle = (descriptors zip handlers).toMap
    (db,descriptorToColumnFamilyHandle)
  }

  def familyHandlerByDescriptorName(name: String, map: scala.collection.Map[ColumnFamilyDescriptor, ColumnFamilyHandle]) = {
    map.find(_._1.columnFamilyName() sameElements name.getBytes) match {
      case Some(handlerToDescriptor) => Some(handlerToDescriptor._2)
      case None => None
    }
  }
}

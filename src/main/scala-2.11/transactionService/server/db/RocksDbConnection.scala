package transactionService.server.db

import java.io.Closeable

import com.twitter.io.TempDirectory
import org.rocksdb._
import transactionService.server.`implicit`.Implicits._

import scala.collection.JavaConverters._


class RocksDbConnection(ttl: Int = -1) extends Closeable{
  val client = TtlDB.open(new Options().setCreateIfMissing(true),RocksDbConnection.path.getAbsolutePath, ttl, false)
  override def close(): Unit = client.close()
}

object RocksDbConnection {
  val path = transactionService.io.FileUtils.createDirectory(resource.DB.TransactionDataDirName)
}

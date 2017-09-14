package benchmark.database

import java.sql.DriverManager

import MySql._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionKey

private object MySql {
  // JDBC URL, username and password of MySQL server

  val host = "localhost"
  val port = 3306

  val dbName = "Transaction"
  val tableName = "Producer_Transaction"

  val url = s"jdbc:mysql://$host:$port/$dbName"
  val user = "root"
  val password = "admin"
}


class MySql
  extends BatchTimeMeasurable
{
  private val connection = DriverManager
    .getConnection(
      url,
      user,
      password
    )

  override def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    val query = s"INSERT INTO $tableName VALUES (?, ?, ?, ?)"
    val statement = connection.prepareStatement(query)
    records.foreach { record =>
      val producerKey =
        ProducerTransactionKey.fromByteArray(record._1)

      statement
        .setInt(1, producerKey.stream)
      statement
        .setInt(2, producerKey.partition)
      statement
        .setLong(3, producerKey.transactionID)
      statement
        .setBytes(4, record._2)

      statement.addBatch()
    }
    scala.util.Try{
      statement.executeBatch()
    } match {
      case scala.util.Success(_) =>
        statement.close()
        true
      case scala.util.Failure(throwable) =>
        throwable.printStackTrace()
        statement.close()
        false
    }
  }

  def close(): Unit =
    connection.close()
}

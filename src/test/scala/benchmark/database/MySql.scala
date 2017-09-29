package benchmark.database

import java.io.File
import java.sql.DriverManager

import MySql._
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord}
import org.apache.commons.io.FileUtils

import scala.collection.mutable

private object MySql {
  // JDBC URL, username and password of MySQL server

  val host = "localhost"
  val port = 6603

  val dbName = "Transaction"
  val tableName = "Producer_Transaction"

  val streamColumn = "stream"
  val partitionColumn = "partition"
  val transactionIdColumn = "transactionId"
  val valueColumn = "value"

  val url = s"jdbc:mysql://$host:$port/$dbName"

  val user = "root"
  val password = "admin"
}


class MySql
  extends AllInOneMeasurable
{
  private def dropTable(): Unit = {
    val connection = DriverManager
      .getConnection(
        url,
        user,
        password
      )
    connection
      .prepareStatement(s"TRUNCATE $tableName")
      .execute()
    connection
      .close()
  }

  dropTable()


  override def putRecords(records: Array[(Array[Byte], Array[Byte])]): Boolean = {
    val connection = DriverManager
      .getConnection(
        url,
        user,
        password
      )
    connection.setAutoCommit(false)

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
      connection.commit()
    } match {
      case scala.util.Success(_) =>
        statement.close()
        connection.close()
        true
      case scala.util.Failure(throwable) =>
        throwable.printStackTrace()
        statement.close()
        connection.close()
        false
    }
  }

  override def dropAllRecords(): Unit = {
    dropTable()
  }

  override def readALLRecords(): Array[(Array[Byte], Array[Byte])] = {
    val connection = DriverManager
      .getConnection(
        url,
        user,
        password
      )

    val query = s"select * from $dbName.$tableName " +
      s"ORDER BY $streamColumn, `$partitionColumn`, $transactionIdColumn;"

    val statement = connection
      .createStatement()

    val set =
      statement.executeQuery(query)

    val buffer =
      mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]

    while (set.next()) {
      val stream =
        set.getInt(streamColumn)
      val partition =
        set.getInt(partitionColumn)
      val transactionId =
        set.getLong(transactionIdColumn)
      val value =
        set.getBytes(valueColumn)

      val binaryKey =
        ProducerTransactionKey(
          stream,
          partition,
          transactionId
        ).toByteArray

      buffer += ((binaryKey, value))
    }

    set.close()
    statement.close()
    connection.close()

    buffer.toArray
  }

  override def readRecords(from: Array[Byte],
                           to: Array[Byte]): Array[(Array[Byte], Array[Byte])] = {
    val fromKey =
      ProducerTransactionKey.fromByteArray(from)
    val toKey =
      ProducerTransactionKey.fromByteArray(to)

    val connection = DriverManager
      .getConnection(
        url,
        user,
        password
      )

    val query =
      s"select * from $dbName.$tableName " +
        "WHERE " +
          s"$streamColumn BETWEEN ${fromKey.stream} AND ${toKey.stream} && " +
          s"`$partitionColumn` BETWEEN ${fromKey.partition} AND ${toKey.partition} && " +
          s"$transactionIdColumn BETWEEN ${fromKey.transactionID} AND ${toKey.transactionID} " +
        "ORDER BY " +
          s"$streamColumn, `$partitionColumn`, $transactionIdColumn;"
    val statement = connection
      .createStatement()

    val set =
      statement.executeQuery(query)

    val buffer =
      mutable.ArrayBuffer.empty[(Array[Byte], Array[Byte])]

    while (set.next()) {
      val stream =
        set.getInt(streamColumn)
      val partition =
        set.getInt(partitionColumn)
      val transactionId =
        set.getLong(transactionIdColumn)
      val value =
        set.getBytes(valueColumn)

      val binaryKey =
        ProducerTransactionKey(
          stream,
          partition,
          transactionId
        ).toByteArray

      buffer += ((binaryKey, value))
    }

    set.close()
    statement.close()
    connection.close()

    buffer.toArray
  }

  override def close(): Unit = {
  }

  override def toString: String = "my_sql_statistic"
}

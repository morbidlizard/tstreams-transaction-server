package com.bwsw.tstreamstransactionserver.netty.server.db.berkeley

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.db.{DbMeta, KeyValueDb, KeyValueDbBatch, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.db.berkeley.BerkeleyDbManager._
import com.sleepycat.je.{DatabaseConfig, Environment, EnvironmentConfig, TransactionConfig}
import org.apache.commons.io.FileUtils

private object BerkeleyDbManager {
  val databaseConfig: DatabaseConfig =
    new DatabaseConfig()
      .setTransactional(true)
      .setAllowCreate(true)
}

final class BerkeleyDbManager(absolutePath: String,
                              dbsMeta: Array[DbMeta])
  extends KeyValueDbManager {
  private val environment = {
    val file = new File(absolutePath)
    FileUtils.forceMkdir(file)

    val config = new EnvironmentConfig()
      .setAllowCreate(true)
      .setTransactional(true)
    new Environment(
      new File(absolutePath),
      config
    )
  }

  private val idToDatabaseMap = {
    dbsMeta.map { meta =>
      val db = environment.openDatabase(
        null,
        meta.name,
        databaseConfig
      )
      (meta.id, db)
    }.toMap
  }


  override def getDatabase(index: Int): KeyValueDb = {
    new BerkeleyDb(
      idToDatabaseMap(index)
    )
  }

  override def newBatch: KeyValueDbBatch = {
    val transaction =
      environment.beginTransaction(
        null,
        TransactionConfig.DEFAULT
      )
    new BerkeleyDbBatch(
      transaction,
      idToDatabaseMap
    )
  }

  override def closeDatabases(): Unit = {
    scala.util.Try {
      idToDatabaseMap
        .values
        .foreach(_.close())
    }.foreach { _ =>
      environment
        .close()
    }
  }
}
